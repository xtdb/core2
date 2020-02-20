(ns crux.datalog.storage
  (:require [clojure.java.io :as io]
            [crux.datalog :as d]
            [crux.datalog.arrow :as da]
            [crux.datalog.hquad-tree :as dhq]
            [crux.datalog.wal :as dw]
            [crux.buffer-pool :as bp]
            [crux.byte-keys :as cbk]
            [crux.object-store :as os])
  (:import java.io.File))

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
    (vals relation-db)))

(defn- write-arrow-children [children {:keys [buffer-pool object-store wal-directory] :as opts}]
  (let [[name hyper-quads path] (dhq/leaf-name->name+hyper-quads+path (d/relation-name (first children)))
        parent-name (dhq/leaf-name name hyper-quads (butlast path))
        arrow-file-view (da/new-arrow-file-view parent-name buffer-pool)
        tmp-file (File/createTempFile parent-name "arrow")]
    (try
      (with-open [in (io/input-stream (da/write-record-batches (map da/->record-batch children) tmp-file))]
        (os/put-object object-store parent-name in))
      (let [children (vec (for [child children]
                            (d/truncate child)))]
        (vec (for [[block-idx child] (map-indexed vector children)
                   :let [child-name (d/relation-name child)]]
               (d/new-parent-child-relation (da/new-arrow-block-relation arrow-file-view block-idx)
                                            (dw/get-wal-relation wal-directory child-name)))))
      (finally
        (.delete tmp-file)))))

(defn- restore-relations [{:keys [wal-directory object-store opts buffer-pool] :as arrow-db}]

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
                  tree (d/relation-by-name arrow-db name)]
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
                                                            (dw/get-wal-relation wal-directory child-name))))
    arrow-db))

(def ^:dynamic *default-options*
  {:wal-directory-factory
   (fn [{:keys [dir wal-directory-dir] :as opts}]
     (assert (or dir wal-directory-dir))
     (dw/new-local-directory-wal-directory (or wal-directory-dir (io/file dir "wals")) dw/new-edn-file-wal da/new-arrow-struct-relation))
   :buffer-pool-factory
   (fn [{:keys [object-store buffer-pool-size-bytes] :as opts}]
     (bp/new-in-memory-pool object-store buffer-pool-size-bytes))
   :object-store-factory
   (fn [{:keys [dir object-store-dir] :as opts}]
     (assert (or dir object-store-dir))
     (os/new-local-directory-object-store (or object-store-dir (io/file dir "objects"))))
   :relation-factory-factory
   (fn [{:keys [wal-directory] :as opts}]
     (let [opts (assoc opts
                       :leaf-tuple-relation-factory
                       (fn [relation-name]
                         (dw/get-wal-relation wal-directory relation-name))
                       :post-process-children-after-split
                       (fn [children]
                         (write-arrow-children children opts)))]
       (fn [relation-name]
         (dhq/new-hyper-quad-tree-relation dhq/default-allocator opts relation-name))))
   :buffer-pool-size-bytes (* 512 1024 1024)
   :leaf-size (* 128 1024)})

(defn new-arrow-db [opts]
  (let [opts (merge *default-options* opts)
        wal-directory ((:wal-directory-factory opts) opts)
        opts (assoc opts :wal-directory wal-directory)
        object-store ((:object-store-factory opts) opts)
        opts (assoc opts :object-store object-store)
        buffer-pool ((:buffer-pool-factory opts) opts)
        relation-factory ((:relation-factory-factory opts) opts)
        opts (assoc opts :relation-factory relation-factory)]
    (restore-relations
     (->ArrowDb {}
                buffer-pool
                object-store
                wal-directory
                (dissoc opts
                        :wal-directory-factory
                        :object-store
                        :object-store-factory
                        :buffer-pool-factory
                        :relation-factory-factory)))))
