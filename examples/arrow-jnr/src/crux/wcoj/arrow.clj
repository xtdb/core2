(ns crux.wcoj.arrow
  (:require [crux.datalog :as cd]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [crux.wcoj :as wcoj]
            [crux.z-curve :as cz]
            [crux.byte-keys :as cbk])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BaseFixedWidthVector BaseIntVector BaseVariableWidthVector BigIntVector BitVector
            ElementAddressableVector Float4Vector Float8Vector FloatingPointVector IntVector TimeStampNanoVector
            ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex FixedSizeListVector StructVector]
           org.apache.arrow.vector.types.pojo.FieldType
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.vector.util.Text
           org.apache.arrow.memory.util.ArrowBufPointer
           [java.util Arrays ArrayList Date List]
           [java.util.function Predicate LongPredicate DoublePredicate]
           java.time.Instant
           java.lang.AutoCloseable
           java.nio.ByteBuffer))


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
                   (.test ^Predicate constraint-fn (arrow->clojure (.getObject ^ElementAddressableVector column idx))))))
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
    this)

  (cardinality [this]
    (.getValueCount this)))

(defn new-arrow-struct-relation
  ^org.apache.arrow.vector.complex.StructVector [relation-name]
  (doto (StructVector/empty (str relation-name) allocator)
    (.setInitialCapacity 0)))

;; Simple N-dimensional quad tree in Rust:
;; https://github.com/reem/rust-n-tree/blob/master/src/lib.rs
;; Python implementation:
;; https://github.com/karimbahgat/Pyqtree/blob/master/pyqtree.py

(defn- try-close [c]
  (when (instance? AutoCloseable c)
    (.close ^AutoCloseable c)))

(def ^:dynamic ^{:tag 'long} *leaf-size* (* 1024 1024))
(def ^:private ^{:tag 'long} root-leaf-idx 0)
(def ^:private ^{:tag 'long} root-parent-node-idx 0)

(def ^:dynamic *internal-leaf-tuple-relation-factory* new-arrow-struct-relation)

(declare insert-tuple)

(deftype HyperQuadTree [^:volatile-mutable ^long dims
                        ^:volatile-mutable ^FixedSizeListVector nodes
                        ^String name
                        ^List leaves]
  wcoj/Relation
  (table-scan [this db]
    (->> (for [leaf leaves
               :when (some? leaf)]
           (wcoj/table-scan leaf db))
         (apply concat)))

  (table-filter [this db var-bindings]
    (->> (for [leaf leaves
               :when (some? leaf)]
           (wcoj/table-filter leaf db var-bindings))
         (apply concat)))

  (insert [this value]
    (when-not (nat-int? dims)
      (let [dims (count value)
            hyper-quads (long (Math/pow 2 dims))]
        (set! (.-dims this) dims)
        (set! (.-nodes this) (doto (FixedSizeListVector/empty "" hyper-quads allocator)
                               (.setInitialCapacity 0)
                               (.setValueCount 0)
                               (.addOrGetVector (FieldType/nullable (.getType Types$MinorType/INT)))))))
    (do (insert-tuple this dims nodes value)
        this))

  (delete [this value]
    (doseq [leaf leaves
            :when (some? leaf)]
      (wcoj/delete leaf value))
    this)

  (cardinality [this]
    (reduce + (map wcoj/cardinality leaves)))

  AutoCloseable
  (close [_]
    (try-close nodes)
    (doseq [leaf leaves]
      (try-close leaf))))

(defn new-hyper-quad-tree-relation ^crux.wcoj.arrow.HyperQuadTree [relation-name]
  (->HyperQuadTree -1 nil relation-name (ArrayList.)))

(defn- tuple->z-address ^long [value]
  (.getLong (ByteBuffer/wrap (cz/bit-interleave (map cbk/->byte-key value)))))

(defn- decode-h-at-level [^long z-address ^long hyper-quads ^long level]
  (let [shift (- Long/SIZE (* (inc level) hyper-quads))]
    (assert (pos? shift))
    (bit-and (unsigned-bit-shift-right z-address shift) (dec hyper-quads))))

(declare insert-tuple)

(defn- decode-node-idx [raw-node-idx]
  (dec (- raw-node-idx)))

(defn- encode-node-idx [node-idx]
  (- (inc node-idx)))

(defn- split-leaf [^HyperQuadTree tree dims ^FixedSizeListVector nodes raw-parent-node-idx leaf-idx]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (try
      (let [new-node-idx (.startNewValue nodes (inc (.getValueCount nodes)))]
        (when (neg? raw-parent-node-idx)
          (.setSafe ^IntVector (.getDataVector nodes)
                    (int (decode-node-idx raw-parent-node-idx))
                    (int (encode-node-idx new-node-idx))))
        (.set leaves leaf-idx nil)
        (doseq [tuple (wcoj/table-scan nil leaf)]
          (insert-tuple tree dims nodes tuple)))
      (finally
        (try-close leaf)))))

(defn- insert-into-leaf [^HyperQuadTree tree dims ^FixedSizeListVector nodes raw-parent-node-idx leaf-idx value]
  (let [leaves ^List (.leaves tree)
        leaf (.get leaves leaf-idx)]
    (if (< (wcoj/cardinality leaf) *leaf-size*)
      (wcoj/insert leaf value)
      (doto tree
        (split-leaf dims nodes raw-parent-node-idx leaf-idx)
        (insert-tuple dims nodes value)))))

(defn- insert-tuple [^HyperQuadTree tree ^long dims ^FixedSizeListVector nodes value]
  (let [leaves ^List (.leaves tree)]
    (if (zero? (.getValueCount nodes))
      (do (when (empty? leaves)
            (.add leaves root-leaf-idx (*internal-leaf-tuple-relation-factory* (.name tree))))
          (insert-into-leaf tree dims nodes root-parent-node-idx root-leaf-idx value))
      (let [z-address (tuple->z-address value)
            hyper-quads (long (Math/pow 2 dims))
            node-vector ^IntVector (.getDataVector nodes)]
        (loop [level 0
               raw-node-idx -1]
          (let [h (decode-h-at-level z-address hyper-quads level)
                parent-node-idx (decode-node-idx raw-node-idx)
                node-idx (+ parent-node-idx h)]
            (if (.isNull node-vector node-idx)
              (let [leaf-idx (.indexOf leaves nil)
                    leaf-idx (if (= -1 leaf-idx)
                               (.size leaves)
                               leaf-idx)]
                (.add leaves leaf-idx (*internal-leaf-tuple-relation-factory* (.name tree)))
                (.setSafe node-vector (int node-idx) leaf-idx)
                (insert-into-leaf tree dims nodes raw-node-idx leaf-idx value))
              (let [child-idx (.get node-vector node-idx)]
                (cond
                  (nat-int? child-idx)
                  (insert-into-leaf tree dims nodes raw-node-idx child-idx value)

                  (neg? child-idx)
                  (recur (inc level) child-idx)

                  :else
                  (throw (IllegalStateException.)))))))))))
