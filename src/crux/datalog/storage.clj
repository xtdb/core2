(ns crux.datalog.storage
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [crux.datalog :as d]
            [crux.datalog.arrow :as da]
            [crux.datalog.hquad-tree :as dhq]
            [crux.datalog.wal :as dw]
            [crux.buffer-pool :as bp]
            [crux.byte-keys :as cbk]
            [crux.io :as cio]
            [crux.object-store :as os]
            [crux.z-curve :as cz])
  (:import clojure.lang.IObj
           [crux.datalog.arrow ArrowBlockRelation ArrowRecordBatchView]
           [org.apache.arrow.vector BitVector FixedSizeBinaryVector VectorSchemaRoot]
           java.io.File
           java.lang.AutoCloseable
           [java.util Arrays Comparator]))

(set! *unchecked-math* :warn-on-boxed)

(deftype ArrowDb [^:volatile-mutable relation-db buffer-pool object-store wal-directory rule-wal-directory options]
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
                (dhq/tuple->z-address x)
                (dhq/tuple->z-address y)))))

(def ^:dynamic ^{:tag 'long} *z-index-byte-width* 16)

(defn- relation->z-index
  (^org.apache.arrow.vector.FixedSizeBinaryVector [relation ^long prefix-length]
   (relation->z-index relation prefix-length *z-index-byte-width*))
  (^org.apache.arrow.vector.FixedSizeBinaryVector [relation ^long  prefix-length ^long byte-width]
   (let [z-index (FixedSizeBinaryVector. (str (d/relation-name relation)) da/default-allocator byte-width)]
     (doseq [^bytes z (if (dhq/z-sorted-map? relation)
                        (keys relation)
                        (map dhq/tuple->z-address (d/table-scan relation {})))
             :let [z (Arrays/copyOfRange z prefix-length (+ prefix-length byte-width))
                   idx (.getValueCount z-index)]]
       (doto z-index
         (.setSafe idx z)
         (.setValueCount (inc idx))))
     z-index)))

(defn- binary-search-z-index ^long [^FixedSizeBinaryVector z-index ^bytes k]
  (with-open [k-vector (FixedSizeBinaryVector. nil (.getAllocator z-index) (.getByteWidth z-index))]
    (doto k-vector
      (.setSafe 0 (Arrays/copyOf k (.getByteWidth z-index)))
      (.setValueCount 1))
    (let [k-pointer (.getDataPointer k-vector 0)]
      (loop [low 0
             high (dec (.getValueCount z-index))]
        (if (<= low high)
          (let [mid (+ low (bit-shift-right (- high low) 1))
                diff (.compareTo k-pointer (.getDataPointer z-index mid))]
            (cond
              (zero? diff)
              mid

              (pos? diff)
              (recur (inc mid) high)

              :else
              (recur low (dec mid))))
          (dec (- low)))))))

(defn- binary-search-idx->pos-idx ^long [^long idx]
  (if (neg? idx)
    (dec (- idx))
    idx))

(defn- z-index->selection-vector ^org.apache.arrow.vector.BitVector [^FixedSizeBinaryVector z-index [^bytes min-z ^bytes max-z :as z-range] ^long dims]
  (let [selection-vector ^BitVector (da/new-selection-vector (.getAllocator z-index) (.getValueCount z-index))
        max-z-idx (binary-search-idx->pos-idx (binary-search-z-index z-index max-z))]
    (dotimes [idx (.getValueCount selection-vector)]
      (.set selection-vector idx 0))
    (loop [idx (binary-search-idx->pos-idx (binary-search-z-index z-index min-z))]
      (when (and (< idx (.getValueCount z-index))
                 (<= idx max-z-idx))
        (if (.isNull z-index idx)
          (recur (inc idx))
          (let [k (.get z-index idx)]
            (if (cz/in-z-range? min-z max-z k dims)
              (do (.set selection-vector idx 1)
                  (recur (inc idx)))
              (when-let [^bytes bigmin (second (cz/z-range-search min-z max-z k dims))]
                (recur (binary-search-idx->pos-idx (binary-search-z-index z-index bigmin)))))))))
    selection-vector))

(deftype ZIndexArrowBlockRelation [^ArrowBlockRelation arrow-block-relation
                                   ^ArrowBlockRelation z-index-arrow-block-relation
                                   ^long prefix-length]
  d/Relation
  (table-scan [this db]
    (d/table-scan arrow-block-relation db))

  (table-filter [this db var-bindings]
    (let [record-batch-view ^ArrowRecordBatchView (nth (.arrow-file arrow-block-relation)
                                                       (.block-idx arrow-block-relation))
          record-batch (.record-batch record-batch-view)
          z-index-record-batch-view ^ArrowRecordBatchView (nth (.arrow-file z-index-arrow-block-relation)
                                                               (.block-idx z-index-arrow-block-relation))
          z-index-record-batch ^VectorSchemaRoot (.record-batch z-index-record-batch-view)
          z-index-column ^FixedSizeBinaryVector (.getVector z-index-record-batch 0)
          z-range (for [^bytes z (dhq/var-bindings->z-range var-bindings)]
                    (Arrays/copyOfRange z prefix-length (+ prefix-length (.getByteWidth z-index-column))))
          dims (count var-bindings)
          z-index-selection-vector ^BitVector (z-index->selection-vector z-index-column
                                                                         z-range
                                                                         dims)]
      (.register da/buffer-cleaner record-batch #(cio/try-close z-index-selection-vector))
      (da/arrow-seq record-batch
                    var-bindings
                    (fn [^long base-offset ^long vector-size]
                      (let [transfter-pair (.getTransferPair z-index-selection-vector (.getAllocator z-index-selection-vector))]
                        (.splitAndTransfer transfter-pair base-offset vector-size)
                        (.getTo transfter-pair))))))

  (insert [this value]
    (throw (UnsupportedOperationException.)))

  (delete [this value]
    (throw (UnsupportedOperationException.)))

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    (d/cardinality arrow-block-relation))

  (relation-name [this]))

(defn- new-arrow-leaf-relation [arrow-file-view arrow-z-index-file-view block-idx wal-directory child-name z-index-prefix-length]
  (d/new-parent-child-relation
   (if arrow-z-index-file-view
     (->ZIndexArrowBlockRelation (da/new-arrow-block-relation arrow-file-view block-idx)
                                 (da/new-arrow-block-relation arrow-z-index-file-view block-idx)
                                 z-index-prefix-length)
     (da/new-arrow-block-relation arrow-file-view block-idx))
   (dw/get-wal-relation wal-directory child-name)
   z-comparator))

(defn- z-index-prefix-length ^long [^long hyper-quads path]
  (let [dims (cz/hyper-quads->dims hyper-quads)]
    (quot (* (count path) dims) Byte/SIZE)))

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
        z-index-prefix-length (z-index-prefix-length hyper-quads path)
        z-index-record-batches (when z-index?
                                 (for [child new-children]
                                   (some-> child (relation->z-index z-index-prefix-length) (da/->record-batch))))]
    (try
      (with-open [in (io/input-stream (da/write-record-batches (for [child new-children]
                                                                 (some-> child (da/->record-batch))) tmp-file))]
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
        (doseq [child (concat new-children z-index-record-batches)]
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
                  z-index-prefix-length (z-index-prefix-length hyper-quads path)]
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
                                        dhq/new-z-sorted-map-relation
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
