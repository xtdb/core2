(ns core2.expression.comparator
  (:require [core2.expression :as expr]
            [core2.types :as types]
            [core2.util :as util])
  (:import (core2.vector IIndirectVector)
           (core2.vector.extensions KeywordType UuidType)
           java.util.HashMap
           java.util.function.IntBinaryOperator
           (org.apache.arrow.vector.types.pojo ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Int ArrowType$Null ArrowType$Timestamp ArrowType$Utf8)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod expr/codegen-mono-call [:compare ArrowType$Bool ArrowType$Bool] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Boolean/compare ~@emitted-args)))
   :return-type types/int-type})

(defmethod expr/codegen-mono-call [:compare ArrowType$Int ArrowType$Int] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Long/compare ~@emitted-args)))
   :return-type types/int-type})

(defmethod expr/codegen-mono-call [:compare ::types/Number ::types/Number] [_]
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Double/compare ~@emitted-args)))
   :return-type types/int-type})

(defmethod expr/codegen-mono-call [:compare ArrowType$Date ArrowType$Date] [_]
  ;; TODO different scales
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Long/compare ~@emitted-args)))
   :return-type types/int-type})

(defmethod expr/codegen-mono-call [:compare ArrowType$Timestamp ArrowType$Timestamp] [_]
  ;; TODO different scales
  {:continue-call (fn [f emitted-args]
                    (f types/int-type
                       `(Long/compare ~@emitted-args)))
   :return-type types/int-type})

(doseq [arrow-type #{ArrowType$Binary ArrowType$Utf8}]
  (defmethod expr/codegen-mono-call [:compare arrow-type arrow-type] [_]
    {:continue-call (fn [f emitted-args]
                      (f types/int-type
                         `(util/compare-nio-buffers-unsigned ~@emitted-args)))
     :return-type types/int-type}))

(doseq [arrow-type #{KeywordType UuidType}]
  (defmethod expr/codegen-mono-call [:compare arrow-type arrow-type] [_]
    {:continue-call (fn [f emitted-args]
                      (f types/int-type `(.compareTo ~@(map #(expr/with-tag % Comparable) emitted-args))))
     :return-type types/int-type}))

(doseq [[f left-type right-type res] [[:compare-nulls-first ArrowType$Null ArrowType$Null 0]
                                      [:compare-nulls-first ArrowType$Null ::types/Object -1]
                                      [:compare-nulls-first ::types/Object ArrowType$Null 1]
                                      [:compare-nulls-last ArrowType$Null ArrowType$Null 0]
                                      [:compare-nulls-last ArrowType$Null ::types/Object 1]
                                      [:compare-nulls-last ::types/Object ArrowType$Null -1]]]
  (defmethod expr/codegen-mono-call [f left-type right-type] [_]
    {:return-type types/int-type
     :continue-call (fn [f _]
                      (f types/int-type res))}))

(doseq [f [:compare-nulls-first :compare-nulls-last]]
  (defmethod expr/codegen-mono-call [f ::types/Object ::types/Object] [expr]
    (expr/codegen-mono-call (assoc expr :f :compare))))

(def ^:private build-comparator
  (-> (fn [left-col-types right-col-types null-ordering]
        (let [left-idx-sym (gensym 'left-idx)
              right-idx-sym (gensym 'right-idx)
              left-col-sym (gensym 'left-col)
              right-col-sym (gensym 'right-col)
              codegen-opts {:var->types {left-col-sym left-col-types
                                         right-col-sym right-col-types}}
              left-boxes (HashMap.)
              {cont-l :continue} (expr/codegen-expr {:op :variable, :variable left-col-sym, :idx left-idx-sym}
                                                    (assoc codegen-opts :return-boxes left-boxes))
              right-boxes (HashMap.)
              {cont-r :continue} (expr/codegen-expr {:op :variable, :variable right-col-sym, :idx right-idx-sym}
                                                    (assoc codegen-opts :return-boxes right-boxes))]
          (-> `(fn [~(-> left-col-sym (expr/with-tag IIndirectVector))
                    ~(-> right-col-sym (expr/with-tag IIndirectVector))]
                 (let [~@(expr/box-bindings (concat (vals left-boxes) (vals right-boxes)))]
                   (reify IntBinaryOperator
                     (~'applyAsInt [_# ~left-idx-sym ~right-idx-sym]
                      ~(cont-l (fn continue-left [left-type left-code]
                                 (cont-r (fn continue-right [right-type right-code]
                                           (let [{cont :continue}
                                                 (expr/codegen-call {:f (case null-ordering
                                                                           :nulls-first :compare-nulls-first
                                                                           :nulls-last :compare-nulls-last)
                                                                      :emitted-args [{:return-types #{left-type}
                                                                                      :continue (fn [f] (f left-type left-code))}
                                                                                     {:return-types #{right-type}
                                                                                      :continue (fn [f] (f right-type right-code))}]})]
                                             (cont (fn [_arrow-type code] code)))))))))))
              #_(doto clojure.pprint/pprint)
              (eval))))
      (memoize)))

(defn ->comparator ^java.util.function.IntBinaryOperator [^IIndirectVector left-col, ^IIndirectVector right-col, null-ordering]
  (let [f (build-comparator (expr/field->value-types (.getField (.getVector left-col)))
                            (expr/field->value-types (.getField (.getVector right-col)))
                            null-ordering)]
    (f left-col right-col)))
