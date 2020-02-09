(ns crux.wcoj.arrow
  (:require [crux.datalog :as cd]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [crux.wcoj :as wcoj])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BaseFixedWidthVector BaseIntVector BaseVariableWidthVector BigIntVector BitVector
            ElementAddressableVector Float4Vector Float8Vector FloatingPointVector IntVector TimeStampNanoVector
            ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex StructVector]
           org.apache.arrow.vector.types.pojo.FieldType
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.vector.util.Text
           org.apache.arrow.memory.util.ArrowBufPointer
           [java.util Arrays Date]
           [java.util.function Predicate LongPredicate DoublePredicate]
           java.time.Instant
           java.nio.charset.Charset))

(def ^:private ^BufferAllocator
  allocator (RootAllocator. Long/MAX_VALUE))

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

(def ^:private ^Charset utf-8 (Charset/forName "UTF-8"))

(defprotocol ArrowToClojure
  (arrow->clojure [this]))

(extend-protocol ArrowToClojure
  (class (byte-array 0))
  (arrow->clojure [this]
    (edn/read-string (String. ^bytes this utf-8)))

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
    (Text. (.getBytes this utf-8)))

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
    (.getBytes (pr-str this) utf-8)))

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

(defn- arrow-seq [^StructVector struct var-bindings]
  (let [struct-batch (VectorSchemaRoot. struct)
        vector-size (min *vector-size* (.getRowCount struct-batch))
        selection-vector (doto (BitVector. "" allocator)
                           (.setValueCount vector-size)
                           (.setInitialCapacity vector-size))
        unify-tuple? (wcoj/contains-duplicate-vars? var-bindings)
        unifier-vector (wcoj/insert
                        (StructVector/empty nil allocator)
                        var-bindings)
        column-filter (unifiers->column-filters unifier-vector var-bindings)
        projection (wcoj/projection var-bindings)]
    (if (zero? (count (.getFieldVectors struct-batch)))
      (repeat vector-size (with-meta [] {::index 0}))
      (->> (for [start-idx (range 0 (.getRowCount struct-batch) vector-size)
                 :let [start-idx (long start-idx)
                       record-batch (if (< (.getRowCount struct-batch) (+ start-idx vector-size))
                                      (.slice struct-batch start-idx)
                                      (.slice struct-batch start-idx vector-size))]]
             (cond->> (selected-indexes record-batch column-filter selection-vector)
               true (selected-tuples record-batch projection start-idx)
               unify-tuple? (filter (partial wcoj/unify var-bindings))))
           (apply concat)))))

(extend-protocol wcoj/Relation
  StructVector
  (table-scan [this db]
    (arrow-seq this (mapv wcoj/ensure-unique-logic-var (repeat (.size this) cd/blank-var))))

  (table-filter [this db var-bindings]
    (arrow-seq this var-bindings))

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
    (doseq [to-delete (arrow-seq this value)
            :let [idx (::index (meta to-delete))]]
      (dotimes [n (.size this)]
        (let [column (.getChildByOrdinal this n)]
          (when (instance? BaseFixedWidthVector column)
            (.setNull ^BaseFixedWidthVector column idx))
          (when (instance? BaseVariableWidthVector column)
            (.setNull ^BaseVariableWidthVector column idx))))
      (.setNull this idx))
    this)

  (cardinality [this]
    (.getValueCount this)))

(defn new-arrow-struct-relation
  ^org.apache.arrow.vector.complex.StructVector [relation-name]
  (doto (StructVector/empty (str relation-name) allocator)
    (.setInitialCapacity 0)))
