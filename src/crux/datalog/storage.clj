(ns crux.datalog.storage
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [crux.datalog :as d]
            [crux.datalog.arrow :as da]
            [crux.datalog.arrow.z-index :as daz]
            [crux.datalog.hquad-tree :as dhq]
            [crux.datalog.wal :as dw]
            [crux.datalog.z-sorted-map :as dz]
            [crux.buffer-pool :as bp]
            [crux.byte-keys :as cbk]
            [crux.io :as cio]
            [crux.object-store :as os])
  (:import clojure.lang.IObj
           java.io.File
           java.lang.AutoCloseable
           java.util.Comparator))

(set! *unchecked-math* :warn-on-boxed)

(deftype ArrowDb [^:volatile-mutable relation-db buffer-pool object-store wal-directory rule-wal-directory options]
  d/Db
  (assertion [this relation-name value]
    (d/ensure-relation this relation-name (:relation-factory options))
    (set! (.-relation-db this)
          (d/assertion relation-db relation-name value))
    this)

  (retraction [this relation-name value]
    (set! (.-relation-db this)
          (d/retraction relation-db relation-name value))
    this)

  (ensure-relation [this relation-name relation-factory]
    (when-not (d/relation-by-name relation-db relation-name)
      (set! (.-relation-db this) (d/ensure-relation relation-db relation-name relation-factory)))
    this)

  (relation-by-name [this relation-name]
    (get relation-db relation-name))

  (relations [this]
    (vals relation-db))

  IObj
  (meta [this]
    (meta relation-db))

  (withMeta [this meta]
    (ArrowDb. (with-meta relation-db meta) buffer-pool object-store wal-directory rule-wal-directory options))

  AutoCloseable
  (close [this]
    (d/close-db relation-db)
    (doseq [dependency [object-store wal-directory rule-wal-directory buffer-pool]]
      (cio/try-close dependency))))

(def ^Comparator z-comparator
  (reify Comparator
    (compare [_ x y]
      (.compare cbk/unsigned-bytes-comparator
                (dz/tuple->z-address x)
                (dz/tuple->z-address y)))))

(defn- new-arrow-leaf-relation [arrow-file-view arrow-z-index-file-view block-idx wal-directory child-name z-index-prefix-length]
  (d/new-parent-child-relation
   (if arrow-z-index-file-view
     (daz/new-z-index-arrow-block-relation arrow-file-view arrow-z-index-file-view block-idx z-index-prefix-length)
     (da/new-arrow-block-relation arrow-file-view block-idx))
   (dw/get-wal-relation wal-directory child-name)
   z-comparator))

(defn- write-arrow-children-on-split [leaf new-children {:keys [buffer-pool object-store wal-directory
                                                                crux.datalog.storage/z-index?] :as opts}]
  (let [parent-name (d/relation-name leaf)
        buffer-name (str parent-name ".arrow")
        z-index-buffer-name  (str parent-name ".idx.arrow")
        arrow-file-view (da/new-arrow-file-view buffer-name buffer-pool)
        arrow-z-index-file-view (when z-index?
                                  (da/new-arrow-file-view z-index-buffer-name buffer-pool))
        tmp-file (File/createTempFile parent-name "arrow")
        [name hyper-quads path] (dhq/leaf-name->name+hyper-quads+path parent-name)
        z-index-prefix-length (daz/z-index-prefix-length hyper-quads path)
        z-index-record-batches (when z-index?
                                 (for [child new-children]
                                   (some-> child (daz/relation->z-index z-index-prefix-length) (da/->record-batch))))
        new-record-batches (for [child new-children]
                             (some-> child (da/->record-batch)))]
    (try
      (with-open [in (io/input-stream (da/write-record-batches new-record-batches tmp-file))]
        (os/put-object object-store buffer-name in))
      (when z-index?
        (let [z-index-tmp-file (File/createTempFile parent-name "idx.arrow")]
          (try
            (with-open [in (io/input-stream (da/write-record-batches z-index-record-batches z-index-tmp-file))]
              (os/put-object object-store z-index-buffer-name in))
            (finally
              (.delete z-index-tmp-file)))))
      (d/truncate leaf)
      (vec (for [[block-idx child] (map-indexed vector new-children)
                 :let [child (when child
                               (d/truncate child))
                       child-name (dhq/leaf-name name hyper-quads (conj path block-idx))]]
             (new-arrow-leaf-relation arrow-file-view arrow-z-index-file-view block-idx wal-directory child-name z-index-prefix-length)))
      (finally
        (doseq [child (concat new-record-batches z-index-record-batches)]
          (cio/try-close child))
        (.delete tmp-file)))))

(defn- restore-relations [^ArrowDb arrow-db]
  (let [{:keys [relation-factory crux.datalog.storage/z-index?] :as options} (.options arrow-db)
        wal-directory (.wal-directory arrow-db)
        root-name->nhp (->> (dw/list-wals (.wal-directory arrow-db))
                            (map dhq/leaf-name->name+hyper-quads+path)
                            (filter (comp empty? last))
                            (group-by first))
        rule-relations (->> (dw/list-wals (.rule-wal-directory arrow-db))
                            (set))
        name->nhp (->> (os/list-objects (.object-store arrow-db))
                       (remove #(re-find #".idx.arrow$" %))
                       (map #(str/replace % #".arrow$" ""))
                       (map dhq/leaf-name->name+hyper-quads+path)
                       (group-by first))
        ^ArrowDb arrow-db (reduce
                           (fn [arrow-db name]
                             (d/ensure-relation arrow-db (symbol name) relation-factory))
                           arrow-db
                           (set (concat (keys root-name->nhp)
                                        (keys name->nhp)
                                        rule-relations)))]
    (doseq [[name [[_ hyper-quads]]] (apply dissoc root-name->nhp (keys name->nhp))
            :let [combined-relation (d/relation-by-name arrow-db (symbol name))
                  tree (:tuples combined-relation)]]
      (dhq/ensure-root-node tree hyper-quads))
    (doseq [[name nhp] name->nhp
            :let [nhp (sort-by (comp count last) nhp)
                  combined-relation (d/relation-by-name arrow-db (symbol name))
                  tree (:tuples combined-relation)]
            [_ hyper-quads path] nhp
            :let [parent-name (dhq/leaf-name name hyper-quads path)
                  buffer-name (str parent-name ".arrow")
                  z-index-buffer-name  (str parent-name ".idx.arrow")
                  arrow-file-view (da/new-arrow-file-view buffer-name (.buffer-pool arrow-db))
                  arrow-z-index-file-view (when z-index?
                                            (da/new-arrow-file-view z-index-buffer-name (.buffer-pool arrow-db)))
                  z-index-prefix-length (daz/z-index-prefix-length hyper-quads path)]
            block-idx (range hyper-quads)
            :let [child-path (conj path block-idx)
                  child-name (dhq/leaf-name name hyper-quads child-path)]]
      (dhq/insert-leaf-at-path tree
                               hyper-quads
                               child-path
                               (new-arrow-leaf-relation arrow-file-view arrow-z-index-file-view block-idx wal-directory child-name z-index-prefix-length)))))

(defn new-in-memory-buffer-pool-factory [{:keys [object-store crux.datalog.storage/in-memory-buffer-pool-size-bytes] :as opts}]
  (assert in-memory-buffer-pool-size-bytes)
  (bp/new-in-memory-pool object-store in-memory-buffer-pool-size-bytes))

(defn new-mmap-buffer-pool-factory [{:keys [object-store crux.datalog.storage/mmap-buffer-pool-size] :as opts}]
  (assert mmap-buffer-pool-size)
  (bp/new-mmap-pool object-store mmap-buffer-pool-size))

(defn new-local-directory-wal-directory-factory [{:crux.datalog.storage/keys [root-dir
                                                                              tuple-wal-local-directory
                                                                              wal-suffix] :as opts}]
  (assert (or root-dir tuple-wal-local-directory))
  (dw/new-local-directory-wal-directory (or tuple-wal-local-directory (io/file root-dir "tuple-wals"))
                                        dw/new-edn-file-wal
                                        dz/new-z-sorted-map-relation
                                        wal-suffix))

(defn new-local-directory-rule-wal-directory-factory [{:crux.datalog.storage/keys [root-dir
                                                                                   rule-wal-local-directory
                                                                                   wal-suffix] :as opts}]
  (assert (or root-dir rule-wal-local-directory))
  (dw/new-local-directory-wal-directory (or rule-wal-local-directory (io/file root-dir "rule-wals"))
                                        dw/new-edn-file-wal
                                        d/new-rule-relation
                                        wal-suffix))

(defn new-local-directory-object-store-factory [{:crux.datalog.storage/keys [root-dir
                                                                             object-store-local-directory] :as opts}]
  (assert (or root-dir object-store-local-directory))
  (os/new-local-directory-object-store (or object-store-local-directory (io/file root-dir "objects"))))

(defn new-hquad-arrow-tuple-relation-factory-factory [{:keys [wal-directory buffer-pool] :as opts}]
  (assert wal-directory)
  (assert buffer-pool)
  (let [opts (assoc opts
                    :crux.datalog.hquad-tree/leaf-tuple-relation-factory
                    (fn [relation-name]
                      (dw/get-wal-relation wal-directory relation-name)))
        opts (assoc opts
                    :crux.datalog.hquad-tree/post-process-children-after-split
                    (fn [old-leaf children]
                      (write-arrow-children-on-split old-leaf children opts)))]
    (fn [relation-name]
      (dhq/new-hyper-quad-tree-relation opts relation-name))))

(defn new-rule-relation-factory-factory [{:keys [rule-wal-directory] :as opts}]
  (fn [relation-name]
    (dw/get-wal-relation rule-wal-directory relation-name)))

(defn new-combined-relation-factory-factory [{:keys [tuple-relation-factory rule-relation-factory] :as opts}]
  (assert tuple-relation-factory)
  (fn [relation-name]
    (d/new-combined-relation relation-name tuple-relation-factory rule-relation-factory)))

(def ^:dynamic *default-options*
  {:crux.datalog.storage/in-memory-buffer-pool-size-bytes (* 128 1024 1024)
   :crux.datalog.storage/mmap-buffer-pool-size 128
   :crux.datalog.storage/wal-suffix ".wal"
   :crux.datalog.storage/z-index? false
   :crux.datalog.hquad-tree/leaf-size (* 32 1024)
   :crux.datalog.hquad-tree/split-leaf-tuple-relation-factory da/new-arrow-struct-relation
   :wal-directory-factory new-local-directory-wal-directory-factory
   :rule-wal-directory-factory new-local-directory-rule-wal-directory-factory
   :buffer-pool-factory new-in-memory-buffer-pool-factory
   :object-store-factory new-local-directory-object-store-factory
   :tuple-relation-factory-factory new-hquad-arrow-tuple-relation-factory-factory
   :rule-relation-factory-factory new-rule-relation-factory-factory
   :relation-factory-factory new-combined-relation-factory-factory})

(defn new-arrow-db ^crux.datalog.storage.ArrowDb [opts]
  (let [opts (merge *default-options* opts)
        wal-directory ((:wal-directory-factory opts) opts)
        opts (assoc opts :wal-directory wal-directory)
        rule-wal-directory ((:rule-wal-directory-factory opts) opts)
        opts (assoc opts :rule-wal-directory rule-wal-directory)
        object-store ((:object-store-factory opts) opts)
        opts (assoc opts :object-store object-store)
        buffer-pool ((:buffer-pool-factory opts) opts)
        opts (assoc opts :buffer-pool buffer-pool)
        tuple-relation-factory ((:tuple-relation-factory-factory opts) opts)
        opts (assoc opts :tuple-relation-factory tuple-relation-factory)
        rule-relation-factory ((:rule-relation-factory-factory opts) opts)
        opts (assoc opts :rule-relation-factory rule-relation-factory)
        relation-factory ((:relation-factory-factory opts) opts)
        opts (assoc opts :relation-factory relation-factory)]
    (doto (->ArrowDb {}
                     buffer-pool
                     object-store
                     wal-directory
                     rule-wal-directory
                     (dissoc opts
                             :wal-directory
                             :wal-directory-factory
                             :rule-wal-directory
                             :rule-wal-directory-factory
                             :object-store
                             :object-store-factory
                             :buffer-pool
                             :buffer-pool-factory
                             :tuple-relation-factory-factory
                             :relation-factory-factory
                             :rule-relation-factory-factory))
      (restore-relations))))
