(ns crux.datalog.storage
  (:require [clojure.java.io :as io]
            [crux.datalog :as d]
            [crux.datalog.arrow :as da]
            [crux.datalog.hquad-tree :as dhq]
            [crux.datalog.wal :as dw]
            [crux.buffer-pool :as bp]
            [crux.byte-keys :as cbk]
            [crux.object-store :as os]))

(defrecord ArrowDb [relation-db buffer-pool object-store wal-directory options]
  d/Db
  (assertion [this relation-name value]
    (update-in (d/ensure-relation this relation-name (:relation-factory options))
               [:relation-db relation-name]
               d/insert
               value))

  (retraction [this relation-name value]
    (update-in this
               [:relation-db relation-name]
               d/delete
               value))

  (ensure-relation [this relation-name relation-factory]
    (cond-> this
      (not (contains? relation-db relation-name)) (assoc-in [:relation-db relation-name] (relation-factory relation-name))))

  (relation-by-name [this relation-name]
    (get relation-db relation-name))

  (relations [this]
    (vals relation-db)))

(defn- restore-relations [arrow-db]
  arrow-db)

(defn- write-arrow-children [children {:keys [object-store wal-directory] :as opts}]
  children)

(def ^:dynamic *default-options*
  {:wal-directory-factory
   (fn [{:keys [dir wal-directory-dir] :as opts}]
     (assert (or dir wal-directory-dir))
     (dw/new-local-directory-wal-directory (or wal-directory-dir (io/file dir "wals"))))
   :buffer-pool-factory
   (fn [{:keys [object-store buffer-pool-size-bytes] :as opts}]
     (bp/new-in-memory-pool object-store buffer-pool-size-bytes))
   :object-store-factory
   (fn [{:keys [dir object-store-dir] :as opts}]
     (assert (or dir object-store-dir))
     (os/new-local-directory-object-store (or object-store-dir (io/file dir "objects"))))
   :relation-factory-factory
   (fn [opts]
     (fn [relation-name]
       (dhq/new-hyper-quad-tree-relation dhq/default-allocator
                                         (assoc opts
                                                :post-process-children-after-split
                                                (fn [children]
                                                  (write-arrow-children children opts)))
                                         relation-name)))
   :buffer-pool-size-bytes (* 512 1024 1024)
   :leaf-size (* 128 1024)
   :leaf-tuple-relation-factory d/new-sorted-set-relation})

(defn new-arrow-db [opts]
  (let [opts (merge *default-options* opts)
        wal-directory ((:wal-directory-factory opts) opts)
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
