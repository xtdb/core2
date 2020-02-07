(ns crux.wcoj.arrow
  (:require [crux.datalog :as cd]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [crux.wcoj :as wcoj])
  (:import [clojure.lang IFn$DO IFn$LO IFn$OLO]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BaseFixedWidthVector BaseIntVector BaseVariableWidthVector BigIntVector BitVector
            ElementAddressableVector Float4Vector Float8Vector FloatingPointVector IntVector TimeStampNanoVector
            ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.StructVector
           org.apache.arrow.vector.types.pojo.FieldType
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.vector.util.Text
           org.apache.arrow.memory.util.ArrowBufPointer
           [java.util Arrays Date]
           java.time.Instant))


(def ^:private ^BufferAllocator
  allocator (RootAllocator. Long/MAX_VALUE))

(defn- init-struct [^StructVector struct column-template]
  (reduce
   (fn [^StructVector struct [idx column-template]]
     (let [column-template (if-let [[[_ value]] (and (cd/logic-var? column-template)
                                                     (:constraints (meta column-template)))]
                             value
                             column-template)

           column-type (.getSimpleName (class column-template))
           [^FieldType field-type ^Class vector-class]
           (case (symbol column-type)
             Integer [(FieldType/nullable (.getType Types$MinorType/INT))
                      IntVector]
             Long [(FieldType/nullable (.getType Types$MinorType/BIGINT))
                   BigIntVector]
             Float [(FieldType/nullable (.getType Types$MinorType/FLOAT4))
                    Float4Vector]
             Double [(FieldType/nullable (.getType Types$MinorType/FLOAT8))
                     Float8Vector]
             String [(FieldType/nullable (.getType Types$MinorType/VARCHAR))
                     VarCharVector]
             Boolean [(FieldType/nullable (.getType Types$MinorType/BIT))
                      BitVector]
             (Date Instant) [(FieldType/nullable (.getType Types$MinorType/TIMESTAMPNANO))
                             TimeStampNanoVector]
             [(FieldType/nullable (.getType Types$MinorType/VARBINARY))
              VarBinaryVector])]
       (doto struct
         (.addOrGet
          (str idx "_" (str/lower-case column-type))
          field-type
          vector-class))))
   struct
   (map-indexed vector column-template)))

(defn- arrow->clojure [value]
  (cond
    (instance? Text value)
    (str value)

    (bytes? value)
    (edn/read-string (String. ^bytes value "UTF-8"))

    :else
    value))

(defn- clojure->arrow [value]
  (cond
    (string? value)
    (Text. (.getBytes ^String value "UTF-8"))

    (boolean? value)
    (if value 1 0)

    (number? value)
    value

    (instance? Date value)
    (clojure->arrow (.toInstant ^Date value))

    (instance? Instant value)
    (+ (* (.getEpochSecond ^Instant value) 1000000000)
       (.getNano ^Instant value))

    :else
    (.getBytes (pr-str value) "UTF-8")))

(defn- insert-clojure-value-into-column [^ValueVector column ^long idx v]
  (if-let [[[_ value]] (and (cd/logic-var? v)
                            (:constraints (meta v)))]
    (insert-clojure-value-into-column column idx value)
    (cond
      (instance? IntVector column)
      (.setSafe ^IntVector column idx ^int (clojure->arrow v))

      (instance? BigIntVector column)
      (.setSafe ^BigIntVector column idx ^long (clojure->arrow v))

      (instance? Float4Vector column)
      (.setSafe ^Float4Vector column idx ^float (clojure->arrow v))

      (instance? Float8Vector column)
      (.setSafe ^Float8Vector column idx ^double (clojure->arrow v))

      (instance? VarCharVector column)
      (.setSafe ^VarCharVector column idx ^Text (clojure->arrow v))

      (instance? BitVector column)
      (.setSafe ^BitVector column idx ^long (clojure->arrow v))

      (instance? TimeStampNanoVector column)
      (.setSafe ^TimeStampNanoVector column idx ^long (clojure->arrow v))

      (instance? VarBinaryVector column)
      (.setSafe ^VarBinaryVector column idx ^bytes (clojure->arrow v))

      :else
      (throw (IllegalArgumentException.)))))

(def ^:private ^{:tag 'long} default-vector-size 128)

(defn- wildcard-column-filter [column ^long idx]
  true)

(defn- unifiers->column-filters [unifier-vector var-bindings]
  (vec (for [[^ElementAddressableVector unify-column var-binding] (map vector unifier-vector var-bindings)]
         (cond
           (cd/logic-var? var-binding)
           (if-let [constraint-fn (:constraint-fn (meta var-binding))]
             (cond
               (and (instance? BaseIntVector unify-column)
                    (instance? IFn$LO constraint-fn))
               (fn [^BaseIntVector column ^long idx]
                 (.invokePrim ^IFn$LO constraint-fn (.getValueAsLong column idx)))

               (and (instance? FloatingPointVector unify-column)
                    (instance? IFn$DO constraint-fn))
               (fn [^FloatingPointVector column ^long idx]
                 (.invokePrim ^IFn$DO constraint-fn (.getValueAsDouble column idx)))

               :else
               (fn [^ElementAddressableVector column ^long idx]
                 (constraint-fn (arrow->clojure (.getObject column idx)))))
             wildcard-filter)

           (instance? BitVector unify-column)
           (let [constant (clojure->arrow var-binding)]
             (fn [^BitVector column ^long idx]
               (= constant (.get column idx))))

           :else
           (let [constant (.getDataPointer ^ElementAddressableVector unify-column 0)
                 pointer (ArrowBufPointer.)]
             (fn [^ElementAddressableVector column ^long idx]
               (= constant (.getDataPointer column idx pointer))))))))

(defn- selected-indexes ^org.apache.arrow.vector.BitVector
  [^VectorSchemaRoot record-batch column-filter-fns ^BitVector selection-vector-out]
  (loop [n 0
         selection-vector selection-vector-out]
    (if (< n (count (.getFieldVectors record-batch)))
      (let [column-filter-fn (get column-filter-fns n)]
        (if (and (pos? n) (= wildcard-column-filter column-filter-fn))
          (recur (inc n) selection-vector)
          (let [column ^ElementAddressableVector (.getVector record-batch n)]
            (recur (inc n)
                   (loop [idx 0
                          selection-vector selection-vector]
                     (if (< idx (.getValueCount column))
                       (recur (inc idx)
                              (cond
                                (.isNull column idx)
                                (doto selection-vector
                                  (.setSafe idx 0))

                                (and (pos? n)
                                     (zero? (.get selection-vector idx)))
                                selection-vector

                                :else
                                (doto selection-vector
                                  (.setSafe idx (if (.invokePrim ^IFn$OLO column-filter-fn column idx)
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
        vector-size (min default-vector-size (.getRowCount struct-batch))
        selection-vector (doto (BitVector. "" allocator)
                           (.setValueCount vector-size)
                           (.setInitialCapacity vector-size))
        unify-tuple? (wcoj/contains-duplicate-vars? var-bindings)
        unifier-vector (wcoj/insert
                        (StructVector/empty nil allocator)
                        var-bindings)
        column-filter-fns (unifiers->column-filters unifier-vector var-bindings)
        projection (wcoj/projection var-bindings)]
    (if (zero? (count (.getFieldVectors struct-batch)))
      (repeat vector-size (with-meta [] {::index 0}))
      (->> (for [start-idx (range 0 (.getRowCount struct-batch) vector-size)
                 :let [start-idx (long start-idx)
                       record-batch (if (< (.getRowCount struct-batch) (+ start-idx vector-size))
                                      (.slice struct-batch start-idx)
                                      (.slice struct-batch start-idx vector-size))]]
             (cond->> (selected-indexes record-batch column-filter-fns selection-vector)
               true (selected-tuples record-batch projection start-idx)
               unify-tuple? (filter (partial wcoj/unify var-bindings))))
           (apply concat)))))

(extend-protocol wcoj/Relation
  StructVector
  (table-scan [this db]
    (arrow-seq this nil))

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
    this))

(defn new-arrow-struct-relation
  ^org.apache.arrow.vector.complex.StructVector [relation-name]
  (doto (StructVector/empty (str relation-name) allocator)
    (.setInitialCapacity 0)))
