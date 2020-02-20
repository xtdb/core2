(ns crux.datalog.arrow
  (:require [crux.datalog.parser :as dp]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [crux.datalog :as d]
            [crux.buffer-pool :as bp])
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
           clojure.lang.Indexed
           java.lang.AutoCloseable
           [java.lang.ref Reference WeakReference]
           [java.io FileInputStream FileOutputStream InputStream OutputStream]
           [java.util Arrays Date List]
           [java.util.function Predicate LongPredicate DoublePredicate]
           java.time.Instant
           java.nio.charset.StandardCharsets
           [java.nio ByteBuffer MappedByteBuffer]
           [java.nio.channels FileChannel FileChannel$MapMode SeekableByteChannel]))

(set! *unchecked-math* :warn-on-boxed)

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
     (let [column-template (if-let [[[_ value]] (and (dp/logic-var? column-template)
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
  (if-let [[[_ v]] (and (dp/logic-var? v)
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
           (dp/logic-var? var-binding)
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
      :crux.datalog/blank-var dp/blank-var
      :crux.datalog/logic-var (let [column (.getVector record-batch n)
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

(defn- arrow-seq [^VectorSchemaRoot record-batch  var-bindings]
  (let [allocator (record-batch-allocator record-batch)
        selection-vector (BitVector. "" allocator)
        unify-tuple? (d/contains-duplicate-vars? var-bindings)
        unifier-vector (d/insert
                        (StructVector/empty nil allocator)
                        var-bindings)
        column-filter (unifiers->column-filters unifier-vector var-bindings)
        projection (d/projection var-bindings)
        vector-size (min *vector-size* (.getRowCount record-batch))
        selection-vector (doto selection-vector
                           (.setValueCount vector-size)
                           (.setInitialCapacity vector-size))]
    (->> (for [start-idx (range 0 (.getRowCount record-batch) vector-size)
               :let [start-idx (long start-idx)
                     record-batch (if (< (.getRowCount record-batch) (+ start-idx vector-size))
                                    (.slice record-batch start-idx)
                                    (.slice record-batch start-idx vector-size))]]
           (if (zero? (count (.getFieldVectors record-batch)))
             (repeat vector-size (with-meta [] {::index 0}))
             (cond->> (selected-indexes record-batch column-filter selection-vector)
               true (selected-tuples record-batch projection start-idx)
               unify-tuple? (filter (partial d/unify var-bindings)))))
         (apply concat))))

(defn write-record-batches ^java.io.File [record-batches f]
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

(def ^:private nio-view-reference-manager
  (reify ReferenceManager
    (deriveBuffer [this source-buffer index length]
      (ArrowBuf. this
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

(defn- arrow-buf-view ^io.netty.buffer.ArrowBuf [^ByteBuffer nio-buffer]
  (when-not (.isDirect nio-buffer)
    (throw (IllegalArgumentException. (str "not a direct buffer: " nio-buffer))))
  (ArrowBuf. nio-view-reference-manager
             nil
             (.capacity nio-buffer)
             (PlatformDependent/directBufferAddress nio-buffer)
             (zero? (.capacity nio-buffer))))

(defn- arrow-record-batch-view ^org.apache.arrow.vector.ipc.message.ArrowRecordBatch [^ArrowBlock block ^ByteBuffer nio-buffer]
  (let [prefix-size (if (= (.getInt nio-buffer (.getOffset block)) MessageSerializer/IPC_CONTINUATION_TOKEN)
                      8
                      4)
        ^RecordBatch batch (.header (Message/getRootAsMessage
                                     (.slice nio-buffer
                                             (+ (.getOffset block) prefix-size)
                                             (- (.getMetadataLength block) prefix-size)))
                                    (RecordBatch.))
        body-buffer (arrow-buf-view (.slice nio-buffer
                                            (+ (.getOffset block)
                                               (.getMetadataLength block))
                                            (.getBodyLength block)))]
    (MessageSerializer/deserializeRecordBatch batch body-buffer)))

(defn- read-schema+record-blocks [^BufferAllocator allocator ^ByteBuffer buffer]
  (with-open [in (bp/new-byte-buffer-seekable-byte-channel buffer)
              reader (ArrowFileReader. in allocator)]
    [(.getSchema (.getVectorSchemaRoot reader))
     (.getRecordBlocks reader)]))

(defn- read-arrow-record-batches
  ([buffer]
   (read-arrow-record-batches default-allocator buffer))
  ([^BufferAllocator allocator buffer]
   (let [[schema record-blocks] (read-schema+record-blocks allocator buffer)]
     (vec (for [block record-blocks
                :let [record-batch (VectorSchemaRoot/create schema allocator)
                      loader (VectorLoader. record-batch)]]
            (do (.load loader (arrow-record-batch-view block buffer))
                record-batch))))))

(deftype ArrowRecordBatchView [record-batch ^:volatile-mutable buffer]
  d/Relation
  (table-scan [this db]
    (d/table-scan record-batch db))

  (table-filter [this db var-bindings]
    (d/table-filter record-batch db var-bindings))

  (insert [this value]
    (throw (UnsupportedOperationException.)))

  (delete [this value]
    (throw (UnsupportedOperationException.)))

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    (d/cardinality record-batch))

  (relation-name [this])

  AutoCloseable
  (close [this]
    (set! (.-buffer this) nil)))

(defrecord ArrowBufferRefAndRecordBatches [^Reference buffer-ref ^List record-batches])

(deftype ArrowFileView [buffer-pool ^String name ^:volatile-mutable ^ArrowBufferRefAndRecordBatches buffer-ref-and-record-batches]
  Indexed
  (nth [this n]
    (if-let [buffer (.get ^Reference (.buffer-ref buffer-ref-and-record-batches))]
      (->ArrowRecordBatchView (nth (.record-batches buffer-ref-and-record-batches) n) buffer)
      (let [new-buffer (bp/get-buffer buffer-pool name)
            new-buffer-ref-and-record-batches (ArrowBufferRefAndRecordBatches.
                                               (WeakReference. new-buffer)
                                               (read-arrow-record-batches new-buffer))]
        (set! (.-buffer-ref-and-record-batches this) new-buffer-ref-and-record-batches)
        (->ArrowRecordBatchView (nth (.record-batches new-buffer-ref-and-record-batches) n) new-buffer))))

  AutoCloseable
  (close [this]
    (set! (.-buffer-ref-and-record-batches this) nil)))

(defn new-arrow-file-view [relation-name buffer-pool]
  (->ArrowFileView buffer-pool relation-name nil))

(defrecord ArrowBlockRelation [^ArrowFileView arrow-file ^long block-idx]
  d/Relation
  (table-scan [this db]
    (d/table-scan (nth arrow-file block-idx) db))

  (table-filter [this db var-bindings]
    (d/table-filter (nth arrow-file block-idx) db var-bindings))

  (insert [this value]
    (throw (UnsupportedOperationException.)))

  (delete [this value]
    (throw (UnsupportedOperationException.)))

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    (d/cardinality (nth arrow-file block-idx)))

  (relation-name [this]
    (str (.name arrow-file) "/" block-idx)))

(defn new-arrow-block-relation [^ArrowFileView arrow-file ^long block-idx]
  (->ArrowBlockRelation arrow-file block-idx))

(extend-protocol d/Relation
  StructVector
  (table-scan [this db]
    (d/table-scan (VectorSchemaRoot. this) db))

  (table-filter [this db var-bindings]
    (d/table-filter (VectorSchemaRoot. this) db var-bindings))

  (insert [this value]
    (if (and (zero? (.size this)) (pos? (count value)))
      (d/insert (init-struct this value) value)
      (let [idx (.getValueCount this)]
        (dotimes [n (.size this)]
          (let [v (get value n)
                column (.getChildByOrdinal this n)]
            (insert-clojure-value-into-column column idx v)))

        (doto this
          (.setIndexDefined idx)
          (.setValueCount (inc idx))))))

  (delete [this value]
    (doseq [to-delete (d/table-filter this nil value)
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

  (relation-name [this]
    (.getName this))

  VectorSchemaRoot
  (table-scan [this db]
    (->> (repeat (count (.getFieldVectors this)) dp/blank-var)
         (mapv d/ensure-unique-logic-var)
         (arrow-seq this)))

  (table-filter [this db var-bindings]
    (arrow-seq this var-bindings))

  (insert [this value]
    (throw (UnsupportedOperationException.)))

  (delete [this value]
    (throw (UnsupportedOperationException.)))

  (truncate [this]
    (throw (UnsupportedOperationException.)))

  (cardinality [this]
    (.getRowCount this))

  (relation-name [this]))

(defn new-arrow-struct-relation
  (^org.apache.arrow.vector.complex.StructVector [relation-name]
   (new-arrow-struct-relation default-allocator relation-name))
  (^org.apache.arrow.vector.complex.StructVector [^BufferAllocator allocator relation-name]
   (doto (StructVector/empty (str relation-name) allocator)
     (.setInitialCapacity 0))))

(defprotocol ToRecordBatch
  (->record-batch [this]))

(extend-protocol ToRecordBatch
  VectorSchemaRoot
  (->record-batch [this]
    this)

  StructVector
  (->record-batch [this]
    (VectorSchemaRoot. this))

  Object
  (->record-batch [this]
    (->record-batch
     (reduce
      d/insert
      (new-arrow-struct-relation (d/relation-name this))
      (d/table-scan this nil)))))
