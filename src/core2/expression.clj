(ns core2.expression
  (:require [clojure.set :as set]
            [clojure.walk :as w]
            [core2.operator.project]
            [core2.types :as types]
            [core2.util :as util])
  (:import core2.operator.project.ProjectionSpec
           core2.select.IVectorSchemaRootSelector
           java.lang.reflect.Method
           java.util.Date
           java.time.LocalDateTime
           org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.types.Types$MinorType
           [org.apache.arrow.vector BigIntVector BitVector FieldVector FixedWidthVector Float8Vector NullVector
            TimeStampMilliVector VarBinaryVector VarCharVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

;; TODO:

;; Normalise constants and get methods for
;; Text/bytes/Dates/Intervals. Support ArrowBufPointers?

;; Add tests for things beyond numbers.

;; Add support for IVectorPredicate, currently used by scan, but these
;; should be replaced by per-vector versions: add and use
;; IVectorSelector instead.

;; Figure out how to use this for metadata.

;; Example of other built-in ops needed are things related to strings,
;; dates, casts and temporal intervals.

(set! *unchecked-math* :warn-on-boxed)

(defn variables [expr]
  (filter symbol? (tree-seq sequential? rest expr)))

(def ^:private arrow-type->vector-type
  {(.getType Types$MinorType/NULL) NullVector
   (.getType Types$MinorType/BIGINT) BigIntVector
   (.getType Types$MinorType/FLOAT8) Float8Vector
   (.getType Types$MinorType/VARBINARY) VarBinaryVector
   (.getType Types$MinorType/VARCHAR) VarCharVector
   (.getType Types$MinorType/TIMESTAMPMILLI) TimeStampMilliVector
   (.getType Types$MinorType/BIT) BitVector
   (.getType Types$MinorType/UNION) DenseUnionVector})

(def ^:private byte-array-class (Class/forName "[B"))

(def ^:private arrow-type->java-type
  {(.getType Types$MinorType/NULL) nil
   (.getType Types$MinorType/BIGINT) Long
   (.getType Types$MinorType/FLOAT8) Double
   (.getType Types$MinorType/VARBINARY) byte-array-class
   (.getType Types$MinorType/VARCHAR) String
   (.getType Types$MinorType/TIMESTAMPMILLI) Date
   (.getType Types$MinorType/BIT) Boolean
   (.getType Types$MinorType/UNION) Object})

(def ^:private type->cast
  {Long 'long
   Double 'double
   byte-array-class 'bytes
   String 'str
   Date 'long
   Boolean 'boolean})

(def ^:private type->boxed-type {Double/TYPE Double
                                 Long/TYPE Long
                                 Boolean/TYPE Boolean})

(def ^:private idx-sym (gensym "idx"))

(defn normalize-union-value [v]
  (cond
    (instance? LocalDateTime v)
    (.getTime (util/local-date-time->date v))
    (instance? Text v)
    (str v)
    :else
    v))

(defn- normalize-expression [expression]
  (w/postwalk #(cond
                 (vector? %)
                 (seq %)
                 (keyword? %)
                 (symbol (name %))
                 :else
                 %)
              expression))

(defn- widen-numeric-types [types]
  (let [types (set types)]
    (cond
      (contains? types Date)
      Date
      (contains? types Double)
      Double
      (contains? types Object)
      Number
      (= types #{Long})
      Long)))

(defmulti codegen (fn [[op & arg+types]]
                    (vec (cons (keyword (name op)) (map second arg+types)))))

(defmethod codegen [:= Number Number] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(== ~@args) Boolean]))

(defmethod codegen [:= Object Object] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(= ~@args) Boolean]))

(defmethod codegen [:= byte-array-class byte-array-class] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(Arrays/equals ~@args) Boolean]))

(defmethod codegen [:!= Object Object] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(not= ~@args) Boolean]))

(defmethod codegen [:!= byte-array-class byte-array-class] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(not (Arrays/equals ~@args)) Boolean]))

(defmethod codegen [:< Number Number] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(< ~@args) Boolean]))

(defmethod codegen [:< Comparable Comparable] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(neg? (compare ~@args)) Boolean]))

(prefer-method codegen [:< Number Number] [:< Comparable Comparable])

(defmethod codegen [:< byte-array-class byte-array-class] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(neg? (Arrays/compareUnsigned ~@args)) Boolean]))

(defmethod codegen [:<= Number Number] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(<= ~@args) Boolean]))

(defmethod codegen [:<= Comparable Comparable] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(not (pos? (compare ~@args))) Boolean]))

(prefer-method codegen [:<= Number Number] [:<= Comparable Comparable])

(defmethod codegen [:<= byte-array-class byte-array-class] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(not (pos? (Arrays/compareUnsigned ~@args))) Boolean]))

(defmethod codegen [:> Number Number] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(> ~@args) Boolean]))

(defmethod codegen [:> Comparable Comparable] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(pos? (compare ~@args)) Boolean]))

(prefer-method codegen [:> Number Number] [:> Comparable Comparable])

(defmethod codegen [:> byte-array-class byte-array-class] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(pos? (Arrays/compareUnsigned ~@args)) Boolean]))

(defmethod codegen [:>= Number Number] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(>= ~@args) Boolean]))

(defmethod codegen [:>= Comparable Comparable] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(not (neg? (compare ~@args))) Boolean]))

(prefer-method codegen [:>= Number Number] [:>= Comparable Comparable])

(defmethod codegen [:>= byte-array-class byte-array-class] [[op & arg+types :as expression]]
  (let [args (map first arg+types)]
    [`(not (neg? (Arrays/compareUnsigned ~@args))) Boolean]))

(doseq [op [:and :or :not]]
  (defmethod codegen [op Boolean Boolean] [[op & arg+types :as expression]]
    (let [args (map first arg+types)]
      [`(~op ~@args) Boolean])))

(doseq [op [:+ :- :*]]
  (defmethod codegen [op Number Number] [[op & arg+types :as expression]]
    (let [[args types] [(map first arg+types) (map second arg+types)]]
      [`(~op ~@args) (widen-numeric-types types)])))

(defmethod codegen [:- Number] [[op & arg+types :as expression]]
  (let [[args types] [(map first arg+types) (map second arg+types)]]
    [`(- ~@args) (widen-numeric-types types)]))

(defmethod codegen [:% Number Number] [[op & arg+types :as expression]]
  (let [[args types] [(map first arg+types) (map second arg+types)]]
    [`(mod ~@args) (widen-numeric-types types)]))

(defmethod codegen [:/ Number Number] [[op & arg+types :as expression]]
  (let [[args types] [(map first arg+types) (map second arg+types)]]
    [`(/ ~@args) (widen-numeric-types types)]))

(defmethod codegen [:/ Long Long] [[op & arg+types :as expression]]
  (let [[args types] [(map first arg+types) (map second arg+types)]]
    [`(quot ~@args) (widen-numeric-types types)]))

(doseq [^Method method (.getDeclaredMethods Math)
        :let [math-op (.getName method)
              boxed-types (map type->boxed-type (.getParameterTypes method))
              boxed-return-type (get type->boxed-type (.getReturnType method))]
        :when (and boxed-return-type (every? some? boxed-types))]
  (defmethod codegen (vec (cons (keyword math-op) boxed-types)) [[_ & arg+types :as expression]]
    (let [args (map first arg+types)]
      [`(~(symbol "Math" math-op) ~@args)
       boxed-return-type])))

(defmethod codegen [:if Boolean Object Object] [[op & arg+types :as expression]]
  (let [[args types] [(map first arg+types) (map second arg+types)]
        types (rest types)
        return-type (if (= 1 (count types))
                      (first types)
                      (or (widen-numeric-types types)
                          Object))
        cast (get type->cast return-type)]
    [(cond->> (cons 'if args)
       cast (list cast))
     return-type]))

(defn- codegen-literal [expression]
  [(if (instance? Date expression)
     (.getTime ^Date expression)
     expression)
   (class expression)])

(defn- codegen-variable [var->type expression]
  (if-let [type (get var->type expression)]
    [(cond
       (= Boolean type)
       `(== 1 (.get ~expression ~idx-sym))
       (= String type)
       `(str (.getObject ~expression ~idx-sym))
       (= Object type)
       `(normalize-union-value (.getObject ~expression ~idx-sym))
       :else
       `(.get ~expression ~idx-sym))
     type]
    (throw (IllegalArgumentException. (str "unknown variable: " expression)))))

(defn- codegen-expression [var->type expression]
  (cond
    (sequential? expression)
    (codegen (cons (first expression)
                   (for [expr (rest expression)]
                     (codegen-expression var->type expr))))
    (symbol? expression)
    (codegen-variable var->type expression)
    :else
    (codegen-literal expression)))

(defn- generate-code [types expression expression-type]
  (let [vars (variables expression)
        expression (normalize-expression expression)
        var->type (zipmap vars (map arrow-type->java-type types))
        [expression return-type] (codegen-expression var->type expression)
        arrow-return-type (types/->arrow-type return-type)
        args (for [[k v] (map vector vars types)]
               (with-meta k {:tag (symbol (.getName ^Class (get arrow-type->vector-type v)))}))]
    (case expression-type
      ::project
      (if (= (.getType Types$MinorType/UNION) arrow-return-type)
        `(fn [[~@args] ^DenseUnionVector acc# ^long row-count#]
           (dotimes [~idx-sym row-count#]
             (let [value# ~expression
                   type-id# (types/arrow-type->type-id (types/->arrow-type (class value#)))
                   offset# (util/write-type-id acc# ~idx-sym type-id#)]
               (types/set-safe! (.getVectorByType acc# type-id#) offset# value#)))
           acc#)
        (let [^Class vector-return-type (get arrow-type->vector-type arrow-return-type)
              return-type-id (types/arrow-type->type-id arrow-return-type)
              inner-acc-sym (with-meta (gensym "inner-acc") {:tag (symbol (.getName vector-return-type))})]
          `(fn [[~@args] ^DenseUnionVector acc# ^long row-count#]
             (let [~inner-acc-sym (.getVectorByType acc# ~return-type-id)]
               (dotimes [~idx-sym row-count#]
                 (let [offset# (util/write-type-id acc# ~idx-sym ~return-type-id)]
                   (.set ~inner-acc-sym offset# ~(cond
                                                   (= BitVector vector-return-type)
                                                   `(if ~expression 1 0)
                                                   (= VarCharVector vector-return-type)
                                                   `(Text. ~expression)
                                                   :else
                                                   expression))))
               acc#))))

      ::select
      (do (assert (= (.getType Types$MinorType/BIT) arrow-return-type))
          `(fn [[~@args] ^RoaringBitmap acc# ^long row-count#]
             (dotimes [~idx-sym row-count#]
               (try
                 (when ~expression
                   (.add acc# ~idx-sym))
                 (catch ClassCastException e#)))
             acc#)))))

(def ^:private memo-generate-code (memoize generate-code))
(def ^:private memo-eval (memoize eval))

(defn- expression-in-vectors [^VectorSchemaRoot in expression]
  (vec (for [var (variables expression)]
         (util/maybe-single-child-dense-union (.getVector in (name var))))))

(defn- vector->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [^ValueVector v]
  (.getType (.getFieldType (.getField v))))

(defn ->expression-projection-spec ^core2.operator.project.ProjectionSpec [col-name expression]
  (reify ProjectionSpec
    (project [_ in allocator]
      (let [in-vecs (expression-in-vectors in expression)
            types (mapv vector->arrow-type in-vecs)
            expr-code (memo-generate-code types expression ::project)
            expr-fn (memo-eval expr-code)
            ^DenseUnionVector acc (.createVector (types/->primitive-dense-union-field col-name) allocator)]
        (expr-fn in-vecs acc (.getRowCount in))))))

(defn ->expression-selector ^core2.select.IVectorSchemaRootSelector [expression]
  (reify IVectorSchemaRootSelector
    (select [_ in]
      (let [in-vecs (expression-in-vectors in expression)
            types (mapv vector->arrow-type in-vecs)
            expr-code (memo-generate-code types expression ::select)
            expr-fn (memo-eval expr-code)
            acc (RoaringBitmap.)]
        (expr-fn in-vecs acc (.getRowCount in))))))
