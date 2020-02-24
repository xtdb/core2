(ns crux.datalog.storage
  (:require [clojure.java.io :as io]
            [crux.datalog :as d]
            [crux.datalog.arrow :as da]
            [crux.datalog.hquad-tree :as dhq]
            [crux.datalog.wal :as dw]
            [crux.buffer-pool :as bp]
            [crux.byte-keys :as cbk]
            [crux.object-store :as os])
  (:import java.io.File
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

  AutoCloseable
  (close [this]
    (d/close-db relation-db)
    (doseq [dependency [object-store wal-directory buffer-pool]]
      (d/try-close dependency))))

(defn- write-arrow-children-on-split [leaf new-children {:keys [buffer-pool object-store wal-directory] :as opts}]
  (let [parent-name (d/relation-name leaf)
        arrow-file-view (da/new-arrow-file-view parent-name buffer-pool)
        tmp-file (File/createTempFile parent-name "arrow")]
    (try
      (with-open [in (io/input-stream (da/write-record-batches (map da/->record-batch new-children) tmp-file))]
        (os/put-object object-store parent-name in))
      (let [empty-children (vec (for [child new-children
                                      :let [child (d/truncate child)]]
                                  ((:crux.datalog.hquad-tree/leaf-tuple-relation-factory opts) (d/relation-name child))))]
        (vec (for [[block-idx child] (map-indexed vector empty-children)]
               (d/new-parent-child-relation (da/new-arrow-block-relation arrow-file-view block-idx) child))))
      (finally
        (.delete tmp-file)))))

(defn- restore-relations [{:keys [wal-directory object-store opts buffer-pool options] :as arrow-db}]
  (let [root-names (->> (dw/list-wals wal-directory)
                        (map dhq/leaf-name->name+hyper-quads+path)
                        (filter (comp empty? last))
                        (map first))
        name->nhp (->> (os/list-objects object-store)
                       (map dhq/leaf-name->name+hyper-quads+path)
                       (group-by first))
        relation-factory (:relation-factory opts)
        arrow-db (reduce
                  (fn [arrow-db name]
                    (d/ensure-relation arrow-db name relation-factory))
                  arrow-db
                  (set (concat root-names (keys name->nhp))))]
    (doseq [[name nhp] name->nhp
            :let [nhp (sort-by count nhp)
                  combined-relation (d/relation-by-name arrow-db name)
                  tree (:tuples combined-relation)]
            [_ hyper-quads path] nhp
            :let [parent-name (dhq/leaf-name tree hyper-quads path)
                  arrow-file-view (da/new-arrow-file-view parent-name buffer-pool)]
            block-idx (range hyper-quads)
            :let [child-path (conj path block-idx)
                  child-name (dhq/leaf-name tree hyper-quads child-path)]]
      (dhq/insert-leaf-at-path tree
                               hyper-quads
                               child-path
                               (d/new-parent-child-relation (da/new-arrow-block-relation arrow-file-view block-idx)
                                                            ((:crux.datalog.hquad-tree/leaf-tuple-relation-factory options) child-name))))))

(def ^:dynamic *default-options*
  {:crux.buffer-pool/size-bytes (* 128 1024 1024)
   :crux.datalog.hquad-tree/leaf-size (* 32 1024)
   :wal-directory-factory
   (fn [{:keys [crux.datomic.storage/root-dir crux.datomic.wal/local-directory] :as opts}]
     (assert (or root-dir local-directory))
     (dw/new-local-directory-wal-directory (or local-directory (io/file root-dir "wals")) dw/new-edn-file-wal dhq/new-z-sorted-set-relation))
   :buffer-pool-factory
   (fn [{:keys [object-store crux.buffer-pool/size-bytes] :as opts}]
     (assert size-bytes)
     (bp/new-in-memory-pool object-store size-bytes))
   :object-store-factory
   (fn [{:keys [crux.datomic.storage/root-dir crux.object-store/local-directory] :as opts}]
     (assert (or root-dir local-directory))
     (os/new-local-directory-object-store (or local-directory (io/file root-dir "objects"))))
   :tuple-relation-factory-factory
   (fn [{:keys [wal-directory] :as opts}]
     (assert wal-directory)
     (let [opts (assoc opts :crux.datalog.hquad-tree/split-leaf-tuple-relation-factory da/new-arrow-struct-relation)
           opts (assoc opts
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
        tuple-relation-factory ((:tuple-relation-factory-factory opts) opts)
        opts (assoc opts :tuple-relation-factory tuple-relation-factory)
        relation-factory ((:relation-factory-factory opts) opts)
        opts (assoc opts :relation-factory relation-factory)]
    (doto (->ArrowDb {}
                     buffer-pool
                     object-store
                     wal-directory
                     (dissoc opts
                             :wal-directory-factory
                             :object-store
                             :object-store-factory
                             :buffer-pool-factory
                             :tuple-relation-factory-factory
                             :relation-factory-factory))
      (restore-relations))))
