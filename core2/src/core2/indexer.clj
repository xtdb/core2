(ns core2.indexer
  (:require core2.buffer-pool
            [core2.metadata :as meta]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           java.io.Closeable
           [java.nio.file Files Path StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.util Date List Map Map$Entry SortedSet]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap ConcurrentSkipListSet]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot VectorLoader VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamReader]
           [org.apache.arrow.vector.types.pojo Field Schema]))

(set! *unchecked-math* :warn-on-boxed)

(definterface IChunkManager
  (^org.apache.arrow.vector.VectorSchemaRoot getLiveRoot [^String fieldName])
  (^java.util.Map getWatermark []))

(definterface TransactionIndexer
  (^core2.tx.TransactionInstant indexTx [^core2.tx.TransactionInstant tx ^java.nio.ByteBuffer txOps]))

(definterface IndexerPrivate
  (^java.nio.ByteBuffer writeColumn [^org.apache.arrow.vector.VectorSchemaRoot live-root])
  (^void closeCols [])
  (^void finishChunk []))

(defn- copy-safe! [^VectorSchemaRoot content-root ^DenseUnionVector src-vec src-idx row-id]
  (let [[^BigIntVector row-id-vec, ^DenseUnionVector field-vec] (.getFieldVectors content-root)
        value-count (.getRowCount content-root)
        type-id (.getTypeId src-vec src-idx)
        offset (util/write-type-id field-vec (.getValueCount field-vec) type-id)]

    (.setSafe row-id-vec value-count ^int row-id)

    (.copyFromSafe (.getVectorByType field-vec type-id)
                   (.getOffset src-vec src-idx)
                   offset
                   (.getVectorByType src-vec type-id))

    (.setRowCount content-root (inc value-count))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-time-field
  (t/->primitive-dense-union-field "_tx-time" #{:timestampmilli}))

(def ^:private timestampmilli-type-id
  (-> (t/primitive-type->arrow-type :timestampmilli)
      (t/arrow-type->type-id)))

(defn ->tx-time-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^Date tx-time]
  (doto ^DenseUnionVector (.createVector tx-time-field allocator)
    (.setValueCount 1)
    (util/write-type-id 0 timestampmilli-type-id)
    (-> (.getTimeStampMilliVector timestampmilli-type-id)
        (.setSafe 0 (.getTime tx-time)))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-id-field
  (t/->primitive-dense-union-field "_tx-id" #{:bigint}))

(def ^:private bigint-type-id
  (-> (t/primitive-type->arrow-type :bigint)
      (t/arrow-type->type-id)))

(defn ->tx-id-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^long tx-id]
  (doto ^DenseUnionVector (.createVector tx-id-field allocator)
    (.setValueCount 1)
    (util/write-type-id 0 bigint-type-id)
    (-> (.getBigIntVector bigint-type-id)
        (.setSafe 0 tx-id))))

(defn- ->live-root [field-name allocator]
  (VectorSchemaRoot/create (Schema. [t/row-id-field (t/->primitive-dense-union-field field-name)]) allocator))

(defn- with-slices [^VectorSchemaRoot root, ^long start-row-id, ^long max-rows-per-block, f]
  (let [^BigIntVector row-id-vec (.getVector root (.getName t/row-id-field))
        row-count (.getRowCount root)]

    (loop [start-idx 0
           target-row-id (+ start-row-id max-rows-per-block)]
      (let [len (loop [len 0]
                  (let [idx (+ start-idx len)]
                    (if (or (>= idx row-count)
                            (>= (.get row-id-vec idx) target-row-id))
                      len
                      (recur (inc len)))))
            end-idx (+ start-idx ^long len)]

        (with-open [sliced-root (util/slice-root root start-idx len)]
          (f sliced-root))

        (when (< end-idx row-count)
          (recur end-idx (+ target-row-id max-rows-per-block)))))))

(definterface IJustIndex
  (^int indexTx [^core2.indexer.IChunkManager chunkMgr, ^org.apache.arrow.memory.BufferAllocator allocator,
                 ^core2.tx.TransactionInstant tx-instant, ^java.nio.ByteBuffer tx-ops,
                 ^long nextRowId]))

(deftype JustIndexer []
  IJustIndex
  (indexTx [_ chunk-mgr allocator tx-instant tx-ops next-row-id]
    (with-open [tx-ops-ch (util/->seekable-byte-channel tx-ops)
                sr (ArrowStreamReader. tx-ops-ch allocator)
                tx-root (.getVectorSchemaRoot sr)
                ^DenseUnionVector tx-id-vec (->tx-id-vec allocator (.tx-id tx-instant))
                ^DenseUnionVector tx-time-vec (->tx-time-vec allocator (.tx-time tx-instant))]

      (.loadNextBatch sr)

      (let [^DenseUnionVector tx-ops-vec (.getVector tx-root "tx-ops")
            op-type-ids (object-array (mapv (fn [^Field field]
                                              (keyword (.getName field)))
                                            (.getChildren (.getField tx-ops-vec))))]
        (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
          (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
                op-vec (.getStruct tx-ops-vec op-type-id)
                per-op-offset (.getOffset tx-ops-vec tx-op-idx)]
            (case (aget op-type-ids op-type-id)
              :put (let [^StructVector document-vec (.getChild op-vec "document" StructVector)
                         row-id (+ next-row-id per-op-offset)]
                     (doseq [^DenseUnionVector value-vec (.getChildrenFromFields document-vec)
                             :when (not (neg? (.getTypeId value-vec per-op-offset)))]

                       (copy-safe! (.getLiveRoot chunk-mgr (.getName (.getField value-vec)))
                                   value-vec per-op-offset row-id))

                     (copy-safe! (.getLiveRoot chunk-mgr (.getName (.getField tx-time-vec)))
                                 tx-time-vec 0 row-id)

                     (copy-safe! (.getLiveRoot chunk-mgr (.getName (.getField tx-id-vec)))
                                 tx-id-vec 0 row-id)))))

        (.getValueCount tx-ops-vec)))))

(defn- chunk-object-key [^long chunk-idx col-name]
  (format "chunk-%08x-%s.arrow" chunk-idx col-name))

(defn- update-watermark [current-watermark ^long chunk-idx ^Map live-roots]
  (reduce
   (fn [acc ^Map$Entry kv]
     (assoc acc
            (chunk-object-key chunk-idx (.getKey kv))
            (.getRowCount ^VectorSchemaRoot (.getValue kv))))
   (empty current-watermark)
   live-roots))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^long max-rows-per-chunk
                  ^long max-rows-per-block
                  ^:volatile-mutable ^long chunk-idx
                  ^:volatile-mutable ^long row-count
                  ^Map live-roots
                  !watermark]

  IChunkManager
  (getLiveRoot [_ field-name]
    (.computeIfAbsent live-roots field-name
                      (util/->jfn
                        (fn [field-name]
                          (->live-root field-name allocator)))))

  (getWatermark [_]
    @!watermark)

  TransactionIndexer
  (indexTx [this tx-instant tx-ops]
    (let [new-row-count (.indexTx (JustIndexer.) this allocator tx-instant tx-ops (+ chunk-idx row-count))]
      (set! (.row-count this) (+ row-count new-row-count)))

    (swap! !watermark update-watermark chunk-idx live-roots)

    ;; TODO better metric here?
    ;; row-id? bytes? tx-id?
    (when (>= row-count max-rows-per-chunk)
      (.finishChunk this))

    tx-instant)

  IndexerPrivate
  (writeColumn [_this live-root]
    (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
      (let [loader (VectorLoader. write-root)]
        (util/build-arrow-ipc-byte-buffer write-root :file
          (fn [write-batch!]
            (with-slices live-root chunk-idx max-rows-per-block
              (fn [sliced-root]
                (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                  (.load loader arb)
                  (write-batch!)))))))))

  (closeCols [_this]
    (doseq [^VectorSchemaRoot live-root (vals live-roots)]
      (util/try-close live-root))

    (.clear live-roots))

  (finishChunk [this]
    (when-not (.isEmpty live-roots)
      (let [futs (vec
                  (for [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
                    (.putObject object-store (chunk-object-key chunk-idx col-name) (.writeColumn this live-root))))]

        @(-> (CompletableFuture/allOf (into-array CompletableFuture futs))
             (util/then-apply
               (fn [_]
                 (.registerNewChunk metadata-mgr live-roots chunk-idx))))

        (.closeCols this)
        (swap! !watermark empty)
        (set! (.chunk-idx this) (+ chunk-idx row-count))
        (set! (.row-count this) 0))))

  Closeable
  (close [this]
    (.closeCols this)))

(defn ->indexer
  (^core2.indexer.Indexer [^BufferAllocator allocator
                           ^ObjectStore object-store
                           ^IMetadataManager metadata-mgr]
   (->indexer allocator object-store metadata-mgr {}))

  (^core2.indexer.Indexer [^BufferAllocator allocator
                           ^ObjectStore object-store
                           ^IMetadataManager metadata-mgr
                           {:keys [max-rows-per-chunk max-rows-per-block]
                            :or {max-rows-per-chunk 10000
                                 max-rows-per-block 1000}}]
   (let [latest-row-id (.latestStoredRowId metadata-mgr)
         next-row-id (if latest-row-id
                       (inc (long latest-row-id))
                       0)]
     (Indexer. allocator
               object-store
               metadata-mgr
               max-rows-per-chunk
               max-rows-per-block
               next-row-id
               0
               (ConcurrentSkipListMap.)
               (atom (sorted-map))))))
