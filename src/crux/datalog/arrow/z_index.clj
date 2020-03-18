(ns crux.datalog.arrow.z-index
  (:require [crux.datalog :as d]
            [crux.datalog.arrow :as da]
            [crux.datalog.z-sorted-map :as dz]
            [crux.io :as cio]
            [crux.z-curve :as cz])
  (:import org.apache.arrow.memory.BufferAllocator
           [crux.datalog.arrow ArrowBlockRelation ArrowFileView ArrowRecordBatchView]
           [org.apache.arrow.vector BitVector FixedSizeBinaryVector VectorSchemaRoot]
           java.util.Arrays
           java.lang.AutoCloseable))

(def ^:dynamic ^{:tag 'long} *z-index-byte-width* 16)

(defn relation->z-index
  (^org.apache.arrow.vector.FixedSizeBinaryVector [relation ^long prefix-length]
   (relation->z-index da/*allocator* relation prefix-length *z-index-byte-width*))
  (^org.apache.arrow.vector.FixedSizeBinaryVector [^BufferAllocator allocator relation ^long  prefix-length ^long byte-width]
   (let [z-index (FixedSizeBinaryVector. (str (d/relation-name relation)) allocator byte-width)]
     (doseq [^bytes z (if (dz/z-sorted-map? relation)
                        (keys relation)
                        (map dz/tuple->z-address (d/table-scan relation {})))
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
    (let [record-batch-view (.getArrowRecordBatchView ^ArrowFileView (.arrow-file arrow-block-relation)
                                                      (.block-idx arrow-block-relation))
          record-batch (.record-batch record-batch-view)
          z-index-record-batch-view (.getArrowRecordBatchView ^ArrowFileView (.arrow-file z-index-arrow-block-relation)
                                                              (.block-idx z-index-arrow-block-relation))
          z-index-record-batch ^VectorSchemaRoot (.record-batch z-index-record-batch-view)
          z-index-column ^FixedSizeBinaryVector (.getVector z-index-record-batch 0)
          z-range (for [^bytes z (dz/var-bindings->z-range var-bindings)]
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
                        (.getTo transfter-pair)))
                    cio/try-close)))

  (insert [this value]
    (throw (UnsupportedOperationException.)))

  (delete [this value]
    (throw (UnsupportedOperationException.)))

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    (d/cardinality arrow-block-relation))

  (relation-name [this])

  AutoCloseable
  (close [this]
    (cio/try-close arrow-block-relation)
    (cio/try-close z-index-arrow-block-relation)))

(defn z-index-prefix-length ^long [^long hyper-quads path]
  (let [dims (cz/hyper-quads->dims hyper-quads)]
    (quot (* (count path) dims) Byte/SIZE)))

(defn new-z-index-arrow-block-relation [arrow-file-view arrow-z-index-file-view block-idx prefix-length]
  (->ZIndexArrowBlockRelation (da/new-arrow-block-relation arrow-file-view block-idx)
                              (da/new-arrow-block-relation arrow-z-index-file-view block-idx)
                              prefix-length))
