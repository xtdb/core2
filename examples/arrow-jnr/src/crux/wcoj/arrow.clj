(ns crux.wcoj.arrow
  (:require [crux.datalog :as cd]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [crux.wcoj :as wcoj])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BaseFixedWidthVector BaseIntVector BaseVariableWidthVector
            BigIntVector BitVector ElementAddressableVector FieldVector Float4Vector Float8Vector
            FloatingPointVector IntVector TimeStampNanoVector ValueVector VarBinaryVector VarCharVector
            VectorLoader VectorUnloader VectorSchemaRoot]
           org.apache.arrow.vector.complex.StructVector
           org.apache.arrow.vector.types.pojo.FieldType
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.vector.util.Text
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowFileWriter]
           [org.apache.arrow.vector.ipc.message ArrowBlock ArrowRecordBatch MessageSerializer]
           [org.apache.arrow.flatbuf Message RecordBatch]
           org.apache.arrow.memory.ReferenceManager
           org.apache.arrow.memory.util.ArrowBufPointer
           io.netty.buffer.ArrowBuf
           io.netty.util.internal.PlatformDependent
           java.lang.AutoCloseable
           [java.io FileInputStream FileOutputStream InputStream OutputStream]
           [java.util Arrays Date]
           [java.util.function Predicate LongPredicate DoublePredicate]
           java.time.Instant
           java.nio.charset.StandardCharsets
           [java.nio ByteBuffer MappedByteBuffer]
           [java.nio.channels FileChannel FileChannel$MapMode SeekableByteChannel]))

(def ^:private ^BufferAllocator
  default-allocator (RootAllocator. Long/MAX_VALUE))

(def ^:private type->arrow-vector-spec
  {Integer
   [(FieldType/nullable (.getType Types$MinorType/INT)) IntVector]
   Long
   [(FieldType/nullable (.getType Types$MinorType/BIGINT)) BigIntVector]
   Float
   [(FieldType/nullable (.getType Types$MinorType/FLOAT4)) Float4Vector]
   Double
   [(FieldType/nullable (.getType Types$MinorType/FLOAT8)) Float8Vector]
   String
   [(FieldType/nullable (.getType Types$MinorType/VARCHAR)) VarCharVector]
   Boolean
   [(FieldType/nullable (.getType Types$MinorType/BIT)) BitVector]
   Date
   [(FieldType/nullable (.getType Types$MinorType/TIMESTAMPNANO)) TimeStampNanoVector]
   Instant
   [(FieldType/nullable (.getType Types$MinorType/TIMESTAMPNANO)) TimeStampNanoVector]})

(def ^:private default-vector-spec
  [(FieldType/nullable (.getType Types$MinorType/VARBINARY)) VarBinaryVector])

(defn- init-struct [^StructVector struct column-template]
  (reduce
   (fn [^StructVector struct [idx column-template]]
     (let [column-template (if-let [[[_ value]] (and (cd/logic-var? column-template)
                                                     (:constraints (meta column-template)))]
                             value
                             column-template)
           column-type (class column-template)
           [^FieldType field-type ^Class vector-class]
           (get type->arrow-vector-spec column-type default-vector-spec)]
       (doto struct
         (.addOrGet
          (str idx "_" (str/lower-case (.getSimpleName column-type)))
          field-type
          vector-class))))
   struct
   (map-indexed vector column-template)))

(defprotocol ArrowToClojure
  (arrow->clojure [this]))

(extend-protocol ArrowToClojure
  (class (byte-array 0))
  (arrow->clojure [this]
    (edn/read-string (String. ^bytes this StandardCharsets/UTF_8)))

  Text
  (arrow->clojure [this]
    (str this))

  Object
  (arrow->clojure [this]
    this))

(defprotocol ClojureToArrow
  (clojure->arrow [this]))

(extend-protocol ClojureToArrow
  String
  (clojure->arrow [this]
    (Text. (.getBytes this StandardCharsets/UTF_8)))

  Boolean
  (clojure->arrow [this]
    (if this 1 0))

  Number
  (clojure->arrow [this]
    this)

  Date
  (clojure->arrow [this]
    (clojure->arrow (.toInstant this)))

  Instant
  (clojure->arrow [this]
    (+ (* (.getEpochSecond this) 1000000000)
       (.getNano this)))

  Object
  (clojure->arrow [this]
    (.getBytes (pr-str this) StandardCharsets/UTF_8)))

(defprotocol ArrowVectorSetter
  (set-column-value [this idx v]))

(extend-protocol ArrowVectorSetter
  IntVector
  (set-column-value [this ^long idx v]
    (.setSafe this idx ^int v))

  BigIntVector
  (set-column-value [this ^long idx v]
    (.setSafe this idx ^long v))

  Float4Vector
  (set-column-value [this ^long idx v]
    (.setSafe this idx ^float v))

  Float8Vector
  (set-column-value [this ^long idx v]
    (.setSafe this idx ^double v))

  VarCharVector
  (set-column-value [this ^long idx v]
    (.setSafe this idx ^Text v))

  BitVector
  (set-column-value [this ^long idx v]
    (.setSafe this idx ^int v))

  TimeStampNanoVector
  (set-column-value [this ^long idx v]
    (.setSafe this idx ^long v))

  VarBinaryVector
  (set-column-value [this ^long idx v]
    (.setSafe this idx ^bytes v)))

(defn- insert-clojure-value-into-column [^ValueVector column ^long idx v]
  (if-let [[[_ v]] (and (cd/logic-var? v)
                        (:constraints (meta v)))]
    (set-column-value column idx (clojure->arrow v))
    (set-column-value column idx (clojure->arrow v))))

(def ^:dynamic ^{:tag 'long} *vector-size* 128)

(definterface ColumnFilter
  (^boolean test [^org.apache.arrow.vector.ValueVector column ^int idx]))

(def ^:private wildcard-column-filter
  (reify ColumnFilter
    (test [_ column idx]
      true)))

(defn- unifiers->column-filters [unifier-vector var-bindings]
  (vec (for [[^ElementAddressableVector unify-column var-binding] (map vector unifier-vector var-bindings)]
         (cond
           (cd/logic-var? var-binding)
           (if-let [constraint-fn (:constraint-fn (meta var-binding))]
             (cond
               (and (instance? BaseIntVector unify-column)
                    (instance? LongPredicate constraint-fn))
               (reify ColumnFilter
                 (test [_ column idx]
                   (.test ^LongPredicate constraint-fn (.getValueAsLong ^BaseIntVector column idx))))

               (and (instance? FloatingPointVector unify-column)
                    (instance? DoublePredicate constraint-fn))
               (reify ColumnFilter
                 (test [_ column idx]
                   (.test ^DoublePredicate constraint-fn (.getValueAsDouble ^FloatingPointVector column idx))))

               :else
               (reify ColumnFilter
                 (test [_ column idx]
                   (.test ^Predicate constraint-fn (arrow->clojure (.getObject ^ValueVector column idx))))))
             wildcard-column-filter)

           (instance? BitVector unify-column)
           (let [constant (clojure->arrow var-binding)]
             (reify ColumnFilter
               (test [_ column idx]
                 (= constant (.get ^BitVector column idx)))))

           :else
           (let [constant (.getDataPointer ^ElementAddressableVector unify-column 0)
                 pointer (ArrowBufPointer.)]
             (reify ColumnFilter
               (test [_ column idx]
                 (= constant (.getDataPointer ^ElementAddressableVector column idx pointer)))))))))

(defn- selected-indexes ^org.apache.arrow.vector.BitVector
  [^VectorSchemaRoot record-batch column-filters ^BitVector selection-vector-out]
  (loop [n 0
         selection-vector selection-vector-out]
    (if (< n (count (.getFieldVectors record-batch)))
      (let [column-filter (get column-filters n)]
        (if (and (pos? n) (= wildcard-column-filter column-filter))
          (recur (inc n) selection-vector)
          (let [column ^ElementAddressableVector (.getVector record-batch n)
                value-count (.getValueCount column)]
            (recur (inc n)
                   (loop [idx (int 0)
                          selection-vector selection-vector]
                     (if (< idx value-count)
                       (recur (unchecked-inc-int idx)
                              (cond
                                (.isNull column idx)
                                (doto selection-vector
                                  (.set idx 0))

                                (and (pos? n)
                                     (zero? (.get selection-vector idx)))
                                selection-vector

                                :else
                                (doto selection-vector
                                  (.set idx (if (.test ^ColumnFilter column-filter column idx)
                                              1
                                              0)))))
                       selection-vector))))))
      selection-vector)))

(defn- project-column [^VectorSchemaRoot record-batch ^long idx ^long n projection]
  (let [p (get projection n)]
    (case p
      :crux.wcoj/blank-var cd/blank-var
      :crux.wcoj/logic-var (let [column (.getVector record-batch n)
                                 value (.getObject column idx)]
                             (arrow->clojure value))
      p)))

(defn- selected-tuples [^VectorSchemaRoot record-batch projection ^long base-offset ^BitVector selection-vector]
  (let [row-count (.getRowCount record-batch)
        column-count (count (.getFieldVectors record-batch))]
    (loop [n 0
           acc []]
      (if (= n column-count)
        acc
        (recur (inc n)
               (loop [selection-offset 0
                      acc acc
                      idx 0]
                 (if (< selection-offset row-count)
                   (if (zero? (.get selection-vector selection-offset))
                     (recur (inc selection-offset)
                            acc
                            idx)
                     (recur (inc selection-offset)
                            (let [value (project-column record-batch selection-offset n projection)]
                              (if (zero? n)
                                (conj acc (with-meta [value] {::index (+ base-offset selection-offset)}))
                                (update acc idx conj value)))
                            (inc idx)))
                   acc)))))))

(defn- record-batch-allocator
  ^org.apache.arrow.memory.BufferAllocator [^VectorSchemaRoot record-batch]
  (if-let [^ValueVector column (first (.getFieldVectors record-batch))]
    (.getAllocator column)
    default-allocator))

(defn- arrow-seq [[^VectorSchemaRoot record-batch :as record-batches] var-bindings]
  (when record-batch
    (let [allocator (record-batch-allocator record-batch)
          selection-vector (BitVector. "" allocator)
          unify-tuple? (wcoj/contains-duplicate-vars? var-bindings)
          unifier-vector (wcoj/insert
                          (StructVector/empty nil allocator)
                          var-bindings)
          column-filter (unifiers->column-filters unifier-vector var-bindings)
          projection (wcoj/projection var-bindings)]
      (->> (for [^VectorSchemaRoot record-batch record-batches
                 :let [vector-size (min *vector-size* (.getRowCount record-batch))
                       selection-vector (doto selection-vector
                                          (.setValueCount vector-size)
                                          (.setInitialCapacity vector-size))]
                 start-idx (range 0 (.getRowCount record-batch) vector-size)
                 :let [start-idx (long start-idx)
                       record-batch (if (< (.getRowCount record-batch) (+ start-idx vector-size))
                                      (.slice record-batch start-idx)
                                      (.slice record-batch start-idx vector-size))]]
             (if (zero? (count (.getFieldVectors record-batch)))
               (repeat vector-size (with-meta [] {::index 0}))
               (cond->> (selected-indexes record-batch column-filter selection-vector)
                 true (selected-tuples record-batch projection start-idx)
                 unify-tuple? (filter (partial wcoj/unify var-bindings)))))
           (apply concat)))))

(defn- write-record-batches ^java.io.File [record-batches f]
  (let [schema (.getSchema ^VectorSchemaRoot (first record-batches))
        f (io/file f)]
    (with-open [record-batch-to (VectorSchemaRoot/create schema default-allocator)
                out (.getChannel (FileOutputStream. f))
                writer (ArrowFileWriter. record-batch-to nil out)]
      (let [loader (VectorLoader. record-batch-to)]
        (doseq [record-batch record-batches]
          (.load loader (.getRecordBatch (VectorUnloader. record-batch)))
          (.writeBatch writer))))
    f))

(defn- new-byte-buffer-seekable-byte-channel ^java.nio.channels.SeekableByteChannel [^ByteBuffer buffer]
  (let [buffer (.slice buffer 0 (.capacity buffer))]
    (proxy [SeekableByteChannel] []
      (isOpen []
        true)

      (close [])

      (read [^ByteBuffer dst]
        (let [src (-> buffer (.slice) (.limit (.remaining dst)))]
          (.put dst src)
          (let [bytes-read (.position src)]
            (.position buffer (+ (.position buffer) bytes-read))
            bytes-read)))

      (position
        ([]
         (.position buffer))
        ([^long new-position]
         (.position buffer new-position)
         this))

      (size []
        (.capacity buffer))

      (write [src]
        (throw (UnsupportedOperationException.)))

      (truncate [size]
        (throw (UnsupportedOperationException.))))))

(def ^:private mmap-reference-manager
  (reify ReferenceManager
    (deriveBuffer [this source-buffer index length]
      (ArrowBuf. mmap-reference-manager
                 nil
                 length
                 (+ (.memoryAddress source-buffer) index)
                 (zero? length)))

    (retain [this])

    (retain [this src-buffer allocator]
      src-buffer)

    (release [this]
      false)

    (getRefCount [this]
      1)))

(defn- mmap-record-batch ^org.apache.arrow.vector.ipc.message.ArrowRecordBatch [^ArrowBlock block ^ByteBuffer nio-buffer]
  (let [prefix-size (if (= (.getInt nio-buffer (.getOffset block)) MessageSerializer/IPC_CONTINUATION_TOKEN)
                      8
                      4)
        ^RecordBatch batch (.header (Message/getRootAsMessage
                                     (.slice nio-buffer
                                             (+ (.getOffset block) prefix-size)
                                             (- (.getMetadataLength block) prefix-size)))
                                    (RecordBatch.))
        body-buffer (ArrowBuf. mmap-reference-manager
                               nil
                               (.getBodyLength block)
                               (+ (PlatformDependent/directBufferAddress nio-buffer)
                                  (.getOffset block)
                                  (.getMetadataLength block))
                               (zero? (.getBodyLength block)))]
    (MessageSerializer/deserializeRecordBatch batch body-buffer)))

(defrecord MmapArrowFileRelation [schema buffer record-batches row-count]
  wcoj/Relation
  (table-scan [this db]
    (->> (for [batch record-batches]
           (wcoj/table-scan batch db))
         (apply concat)))

  (table-filter [this db var-bindings]
    (->> (for [batch record-batches]
           (wcoj/table-filter batch db var-bindings))
         (apply concat)))

  (insert [this value]
    (throw (UnsupportedOperationException.)))

  (delete [this value]
    (throw (UnsupportedOperationException.)))

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    row-count)

  AutoCloseable
  (close [_]
    (doseq [batch record-batches]
      (wcoj/try-close batch))
    (PlatformDependent/freeDirectBuffer buffer)))

(defn- read-schema+record-blocks [^BufferAllocator allocator ^ByteBuffer buffer]
  (with-open [in (new-byte-buffer-seekable-byte-channel buffer)
              reader (ArrowFileReader. in allocator)]
    [(.getSchema (.getVectorSchemaRoot reader))
     (.getRecordBlocks reader)]))

(defn- mmap-file ^java.nio.MappedByteBuffer [f]
  (with-open [in (.getChannel (FileInputStream. (io/file f)))]
    (.map in FileChannel$MapMode/READ_ONLY 0 (.size in))))

(defn new-mmap-arrow-file-relation
  (^crux.wcoj.arrow.MmapArrowFileRelation [f]
   (new-mmap-arrow-file-relation default-allocator f))
  (^crux.wcoj.arrow.MmapArrowFileRelation [^BufferAllocator allocator f]
   (let [buffer (mmap-file f)
         [schema record-blocks] (read-schema+record-blocks allocator buffer)
         record-batches (vec (for [block record-blocks
                                   :let [record-batch (VectorSchemaRoot/create schema allocator)
                                         loader (VectorLoader. record-batch)]]
                               (do (.load loader (mmap-record-batch block buffer))
                                   record-batch)))
         row-count (->> (for [^VectorSchemaRoot batch record-batches]
                          (.getRowCount batch))
                        (reduce +))]
     (->MmapArrowFileRelation schema
                              buffer
                              record-batches
                              row-count))))

(defrecord ParentChildRelation [deletion-set parent child]
  wcoj/Relation
  (table-scan [this db]
    (concat (cond->> (wcoj/table-scan parent db)
              (seq deletion-set) (remove deletion-set))
            (wcoj/table-scan child db)))

  (table-filter [this db var-bindings]
    (concat (cond->> (wcoj/table-filter parent db var-bindings)
              (seq deletion-set) (remove deletion-set))
            (wcoj/table-filter child db var-bindings)))

  (insert [this value]
    (-> this
        (update :child wcoj/insert value)
        (update :deletion-set disj value)))

  (delete [this value]
    (cond-> (update this :child wcoj/delete value)
      (seq (wcoj/table-filter parent nil value)) (update :deletion-set conj value)))

  (truncate [this]
    (-> this
        (update :child wcoj/truncate)
        (update :deletion-set empty)))

  (cardinality [this]
    (+ (wcoj/cardinality parent) (wcoj/cardinality child)))

  AutoCloseable
  (close [_]
    (wcoj/try-close child)
    (wcoj/try-close parent)))

(defn new-parent-child-relation [parent child]
  (->ParentChildRelation #{} parent child))

(extend-protocol wcoj/Relation
  StructVector
  (table-scan [this db]
    (wcoj/table-scan (VectorSchemaRoot. this) db))

  (table-filter [this db var-bindings]
    (wcoj/table-filter (VectorSchemaRoot. this) db var-bindings))

  (insert [this value]
    (if (and (zero? (.size this)) (pos? (count value)))
      (wcoj/insert (init-struct this value) value)
      (let [idx (.getValueCount this)]
        (dotimes [n (.size this)]
          (let [v (get value n)
                column (.getChildByOrdinal this n)]
            (insert-clojure-value-into-column column idx v)))

        (doto this
          (.setIndexDefined idx)
          (.setValueCount (inc idx))))))

  (delete [this value]
    (doseq [to-delete (wcoj/table-filter this nil value)
            :let [idx (::index (meta to-delete))]]
      (dotimes [n (.size this)]
        (let [column (.getChildByOrdinal this n)]
          (when (instance? BaseFixedWidthVector column)
            (.setNull ^BaseFixedWidthVector column idx))
          (when (instance? BaseVariableWidthVector column)
            (.setNull ^BaseVariableWidthVector column idx))))
      (.setNull this idx))
    this)

  (truncate [this]
    (doto this
      (.clear)))

  (cardinality [this]
    (.getValueCount this))

  VectorSchemaRoot
  (table-scan [this db]
    (->> (repeat (count (.getFieldVectors this)) cd/blank-var)
         (mapv wcoj/ensure-unique-logic-var)
         (arrow-seq [this])))

  (table-filter [this db var-bindings]
    (arrow-seq [this] var-bindings))

  (insert [this value]
    (throw (UnsupportedOperationException.)))

  (delete [this value]
    (throw (UnsupportedOperationException.)))

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    (.getRowCount this)))

(defn new-arrow-struct-relation
  (^org.apache.arrow.vector.complex.StructVector [relation-name]
   (new-arrow-struct-relation default-allocator relation-name))
  (^org.apache.arrow.vector.complex.StructVector [^BufferAllocator allocator relation-name]
   (doto (StructVector/empty (str relation-name) allocator)
     (.setInitialCapacity 0))))
