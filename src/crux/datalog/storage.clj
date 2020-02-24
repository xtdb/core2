(ns crux.datalog.storage
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [crux.datalog :as d]
            [crux.datalog.arrow :as da]
            [crux.datalog.hquad-tree :as dhq]
            [crux.datalog.wal :as dw]
            [crux.buffer-pool :as bp]
            [crux.byte-keys :as cbk]
            [crux.object-store :as os])
  (:import clojure.lang.IObj
           java.io.File
           java.lang.AutoCloseable))

(deftype ArrowDb [^:volatile-mutable relation-db buffer-pool object-store wal-directory options]
  d/Db
  (assertion [this relation-name value]
    (d/ensure-relation this relation-name (:relation-factory options))
    (set! (.-relation-db this)
          (update relation-db
                  relation-name
                  d/insert
                  value))
    this)

  (retraction [this relation-name value]
    (set! (.-relation-db this)
          (update relation-db
                  relation-name
                  d/delete
                  value))
    this)

  (ensure-relation [this relation-name relation-factory]
    (when-not (contains? relation-db relation-name)
      (set! (.-relation-db this) (assoc relation-db relation-name (relation-factory relation-name))))
    this)

  (relation-by-name [this relation-name]
    (get relation-db relation-name))

  (relations [this]
    (vals relation-db))

  IObj
  (meta [this]
    (meta relation-db))

  (withMeta [this meta]
    (->ArrowDb (with-meta relation-db meta) buffer-pool object-store wal-directory options))

  AutoCloseable
  (close [this]
    (d/close-db relation-db)
    (doseq [dependency [object-store wal-directory buffer-pool]]
      (d/try-close dependency))))

(defn- write-arrow-children-on-split [leaf new-children {:keys [buffer-pool object-store wal-directory] :as opts}]
  (let [parent-name (d/relation-name leaf)
        arrow-file-view (da/new-arrow-file-view parent-name buffer-pool)
        tmp-file (File/createTempFile parent-name "arrow")
        [name hyper-quads path] (dhq/leaf-name->name+hyper-quads+path parent-name)]
    (try
      (with-open [in (io/input-stream (da/write-record-batches (for [child new-children]
                                                                 (some-> child da/->record-batch)) tmp-file))]
        (os/put-object object-store (str parent-name ".arrow") in))
      (vec (for [[block-idx child] (map-indexed vector new-children)
                 :let [child (when child
                               (d/truncate child))
                       child-name (dhq/leaf-name name hyper-quads (conj path block-idx))
                       child (dw/get-wal-relation wal-directory child-name)]]
             (d/new-parent-child-relation (da/new-arrow-block-relation arrow-file-view block-idx) child)))
      (finally
        (.delete tmp-file)))))

(defn- restore-relations [^ArrowDb arrow-db]
  (let [{:keys [relation-factory] :as options} (.options arrow-db)
        root-name->nhp (->> (dw/list-wals (.wal-directory arrow-db))
                            (map #(str/replace % #".wal$" ""))
                            (map dhq/leaf-name->name+hyper-quads+path)
                            (filter (comp empty? last))
                            (group-by first))
        name->nhp (->> (os/list-objects (.object-store arrow-db))
                       (map #(str/replace % #".arrow$" ""))
                       (map dhq/leaf-name->name+hyper-quads+path)
                       (group-by first))
        ^ArrowDb arrow-db (reduce
                           (fn [arrow-db name]
                             (d/ensure-relation arrow-db (symbol name) relation-factory))
                           arrow-db
                           (set (concat (keys root-name->nhp)
                                        (keys name->nhp))))]
    (doseq [[name [[_ hyper-quads]]] (apply dissoc root-name->nhp (keys name->nhp))
            :let [combined-relation (d/relation-by-name arrow-db (symbol name))
                  tree (:tuples combined-relation)]]
      (dhq/init-hyper-quads tree hyper-quads)
      (dhq/ensure-root-node tree))
    (doseq [[name nhp] name->nhp
            :let [nhp (sort-by (comp count last) nhp)
                  combined-relation (d/relation-by-name arrow-db (symbol name))
                  tree (:tuples combined-relation)]
            [_ hyper-quads path] nhp
            :let [parent-name (dhq/leaf-name name hyper-quads path)
                  arrow-file-view (da/new-arrow-file-view parent-name (.buffer-pool arrow-db))]
            block-idx (range hyper-quads)
            :let [child-path (conj path block-idx)
                  child-name (dhq/leaf-name name hyper-quads child-path)]]
      (dhq/insert-leaf-at-path tree
                               hyper-quads
                               child-path
                               (d/new-parent-child-relation (da/new-arrow-block-relation arrow-file-view block-idx)
                                                            (dw/get-wal-relation (.wal-directory arrow-db) child-name))))))

(def ^:dynamic *default-options*
  {:crux.buffer-pool/size-bytes (* 128 1024 1024)
   :crux.datalog.hquad-tree/leaf-size (* 32 1024)
   :crux.datalog.hquad-tree/split-leaf-tuple-relation-factory da/new-arrow-struct-relation
   :wal-directory-factory
   (fn [{:keys [crux.datalog.storage/root-dir crux.datalog.wal/local-directory] :as opts}]
     (assert (or root-dir local-directory))
     (dw/new-local-directory-wal-directory (or local-directory (io/file root-dir "wals")) dw/new-edn-file-wal dhq/new-z-sorted-set-relation))
   :buffer-pool-factory
   (fn [{:keys [object-store crux.buffer-pool/size-bytes] :as opts}]
     (assert size-bytes)
     (bp/new-in-memory-pool object-store size-bytes))
   :object-store-factory
   (fn [{:keys [crux.datalog.storage/root-dir crux.object-store/local-directory] :as opts}]
     (assert (or root-dir local-directory))
     (os/new-local-directory-object-store (or local-directory (io/file root-dir "objects"))))
   :tuple-relation-factory-factory
   (fn [{:keys [wal-directory buffer-pool] :as opts}]
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
   :relation-factory-factory
   (fn [{:keys [tuple-relation-factory] :as opts}]
     (assert tuple-relation-factory)
     (fn [relation-name]
       (d/new-combined-relation relation-name tuple-relation-factory)))})

(defn new-arrow-db ^crux.datalog.storage.ArrowDb [opts]
  (let [opts (merge *default-options* opts)
        wal-directory ((:wal-directory-factory opts) opts)
        opts (assoc opts :wal-directory wal-directory)
        object-store ((:object-store-factory opts) opts)
        opts (assoc opts :object-store object-store)
        buffer-pool ((:buffer-pool-factory opts) opts)
        opts (assoc opts :buffer-pool buffer-pool)
        tuple-relation-factory ((:tuple-relation-factory-factory opts) opts)
        opts (assoc opts :tuple-relation-factory tuple-relation-factory)
        relation-factory ((:relation-factory-factory opts) opts)
        opts (assoc opts :relation-factory relation-factory)]
    (doto (->ArrowDb {}
                     buffer-pool
                     object-store
                     wal-directory
                     (dissoc opts
                             :wal-directory
                             :wal-directory-factory
                             :object-store
                             :object-store-factory
                             :buffer-pool
                             :buffer-pool-factory
                             :tuple-relation-factory-factory
                             :relation-factory-factory))
      (restore-relations))))
