(ns core2.expression
  (:require [clojure.walk :as w]
            [core2.types :as types]
            [core2.util :as util])
  (:import core2.operator.project.ProjectionSpec
           org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.types.Types$MinorType
           [org.apache.arrow.vector BigIntVector BitVector FieldVector Float8Vector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector))

(set! *unchecked-math* :warn-on-boxed)

(defn variables [expr]
  (filter symbol? (tree-seq sequential? rest expr)))

(defn- infer-return-type [types expression]
  (if-let [tag (:tag expression)]
    (types/->arrow-type tag)
    (cond
      (= #{Float8Vector} types) (.getType Types$MinorType/FLOAT8)
      (= #{BigIntVector Float8Vector} types) (.getType Types$MinorType/FLOAT8)
      (= #{BigIntVector} types) (.getType Types$MinorType/BIGINT)
      (= #{BitVector} types) (.getType Types$MinorType/BIT))))

(def arrow-type->vector-type
  {(.getType Types$MinorType/BIGINT) BigIntVector
   (.getType Types$MinorType/FLOAT8) Float8Vector
   (.getType Types$MinorType/BIT) BitVector})

(defn- primitive-vector-type? [^Class type]
  (= "org.apache.arrow.vector" (.getPackageName type)))

(defn- generate-code [types expression]
  (let [vars (variables expression)
        arrow-return-type (infer-return-type (set types) expression)
        ^Class vector-return-type (get arrow-type->vector-type arrow-return-type)
        inner-acc-sym (with-meta (gensym 'inner-acc) {:tag (symbol (.getName vector-return-type))})
        return-type-id (types/arrow-type->type-id arrow-return-type)
        idx-sym (gensym 'idx)
        var->type (zipmap vars types)
        expanded-expression (w/postwalk
                             #(if-let [type (get var->type %)]
                                (cond
                                  (= BitVector type)
                                  `(= 1 (.get ~% ~idx-sym))
                                  (primitive-vector-type? type)
                                  `(.get ~% ~idx-sym)
                                  :else
                                  `(.getObject ~% ~idx-sym))
                                (cond
                                  (vector? %)
                                  (seq %)
                                  (keyword? %)
                                  (symbol (name %))
                                  :else
                                  %))
                             expression)
        expanded-expression (if (= BitVector vector-return-type)
                              `(if ~expanded-expression 1 0)
                              expanded-expression)]
    `(fn [[~@(for [[k ^Class v] (map vector vars types)]
               (with-meta k {:tag (symbol (.getName v))}))]
          ^DenseUnionVector acc#
          ^long row-count#]
       (let [~inner-acc-sym (.getVectorByType acc# ~return-type-id)]
         (dotimes [~idx-sym row-count#]
           (let [offset# (util/write-type-id acc# ~idx-sym ~return-type-id)]
             (.set ~inner-acc-sym offset# ~expanded-expression))))
       acc#)))

(def ^:private memo-generate-code (memoize generate-code))
(def ^:private memo-eval (memoize eval))

(defn ->expression-projection-spec ^core2.operator.project.ProjectionSpec [col-name expression]
  (reify ProjectionSpec
    (project [_ in allocator]
      (let [in-vecs (vec (for [var (variables expression)]
                           (util/maybe-single-child-dense-union (.getVector in (name var)))))
            types (mapv class in-vecs)
            inner-expr-code (memo-generate-code types expression)
            inner-expr-fn (memo-eval inner-expr-code)
            ^DenseUnionVector acc (.createVector (types/->primitive-dense-union-field col-name) allocator)]
        (inner-expr-fn in-vecs acc (.getRowCount in))))))
