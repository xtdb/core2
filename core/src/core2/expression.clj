(ns core2.expression
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [core2.expression.macro :as macro]
            [core2.expression.walk :as walk]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [core2.error :as err])
  (:import (clojure.lang Keyword MapEntry)
           (core2.operator IProjectionSpec IRelationSelector)
           (core2.types LegType)
           (core2.vector IIndirectRelation IIndirectVector IRowCopier IVectorWriter)
           (core2.vector.extensions KeywordType UuidType)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Clock Duration Instant LocalDate ZonedDateTime ZoneId ZoneOffset Period)
           (java.time.temporal ChronoField ChronoUnit)
           (java.util Date HashMap LinkedList Arrays)
           (java.util.function IntUnaryOperator Function)
           (java.util.stream IntStream)
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector BigIntVector BitVector DurationVector FieldVector IntVector ValueVector PeriodDuration)
           (org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector StructVector)
           (org.apache.arrow.vector.types DateUnit TimeUnit Types Types$MinorType IntervalUnit)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Duration ArrowType$ExtensionType ArrowType$FixedSizeBinary ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$Null ArrowType$Timestamp ArrowType$Utf8 Field FieldType ArrowType$Time ArrowType$Interval)
           (java.util.regex Pattern)
           (org.apache.commons.codec.binary Hex)
           (core2 StringUtil)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti parse-list-form
  (fn [[f & args] env]
    f)
  :default ::default)

(defn form->expr [form {:keys [col-names params locals] :as env}]
  (cond
    (symbol? form) (cond
                     (contains? locals form) {:op :local, :local form}
                     (contains? params form) (let [param-v (get params form)]
                                               {:op :param, :param form,
                                                :param-type (.arrowType (types/value->leg-type param-v))
                                                :param-class (class param-v)})
                     (contains? col-names form) {:op :variable, :variable form}
                     :else (throw (err/illegal-arg :unknown-symbol
                                                   {::err/message (format "Unknown symbol: '%s'" form)
                                                    :symbol form})))

    (map? form) (do
                  (when-not (every? keyword? (keys form))
                    (throw (IllegalArgumentException. (str "keys to struct must be keywords: " (pr-str form)))))
                  {:op :struct,
                   :entries (->> (for [[k v-form] form]
                                   (MapEntry/create k (form->expr v-form env)))
                                 (into {}))})

    (vector? form) {:op :list
                    :elements (mapv #(form->expr % env) form)}

    (seq? form) (parse-list-form form env)

    :else {:op :literal, :literal form}))

(defmethod parse-list-form 'if [[_ & args :as form] env]
  (when-not (= 3 (count args))
    (throw (IllegalArgumentException. (str "'if' expects 3 args: " (pr-str form)))))

  (let [[pred then else] args]
    {:op :if,
     :pred (form->expr pred env),
     :then (form->expr then env),
     :else (form->expr else env)}))

(defmethod parse-list-form 'let [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (IllegalArgumentException. (str "'let' expects 2 args - bindings + body"
                                           (pr-str form)))))

  (let [[bindings body] args]
    (when-not (or (nil? bindings) (sequential? bindings))
      (throw (IllegalArgumentException. (str "'let' expects a sequence of bindings: "
                                             (pr-str form)))))

    (if-let [[local expr-form & more-bindings] (seq bindings)]
      (do
        (when-not (symbol? local)
          (throw (IllegalArgumentException. (str "bindings in `let` should be symbols: "
                                                 (pr-str local)))))
        {:op :let
         :local local
         :expr (form->expr expr-form env)
         :body (form->expr (list 'let more-bindings body)
                           (update env :locals (fnil conj #{}) local))})

      (form->expr body env))))

(defn- ->dot-expr [struct-expr field-form env]
  (if (symbol? field-form)
    {:op :dot-const-field
     :struct-expr struct-expr
     :field field-form}
    {:op :dot
     :struct-expr struct-expr
     :field-expr (form->expr field-form env)}))

(defmethod parse-list-form '. [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (IllegalArgumentException. (str "'.' expects 2 args: " (pr-str form)))))

  (let [[struct field] args]
    (->dot-expr (form->expr struct env) field env)))

(defmethod parse-list-form '.. [[_ & args :as form] env]
  (let [[struct & fields] args]
    (when-not (seq fields)
      (throw (IllegalArgumentException. (str "'..' expects at least 2 args: " (pr-str form)))))
    (when-not (every? symbol? fields)
      (throw (IllegalArgumentException. (str "'..' expects symbol fields: " (pr-str form)))))
    (reduce (fn [struct-expr field-form]
              (->dot-expr struct-expr field-form env))
            (form->expr struct env)
            (rest args))))

(defmethod parse-list-form 'nth [[_ & args :as form] env]
  (when-not (= 2 (count args))
    (throw (IllegalArgumentException. (str "'nth' expects 2 args: " (pr-str form)))))

  (let [[coll-form n-form] args]
    (if (integer? n-form)
      {:op :nth-const-n
       :coll-expr (form->expr coll-form env)
       :n n-form}
      {:op :nth
       :coll-expr (form->expr coll-form env)
       :n-expr (form->expr n-form env)})))

(defmethod parse-list-form ::default [[f & args] env]
  {:op :call, :f f, :args (mapv #(form->expr % env) args)})

(defn with-tag [sym tag]
  (-> sym
      (vary-meta assoc :tag (if (symbol? tag)
                              tag
                              (symbol (.getName ^Class tag))))))

(def type->cast
  {ArrowType$Bool/INSTANCE 'boolean
   (.getType Types$MinorType/TINYINT) 'byte
   (.getType Types$MinorType/SMALLINT) 'short
   (.getType Types$MinorType/INT) 'int
   types/bigint-type 'long
   types/float8-type 'double
   types/timestamp-micro-tz-type 'long
   types/duration-micro-type 'long})

(def idx-sym (gensym 'idx))
(def rel-sym (gensym 'rel))
(def params-sym (gensym 'params))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti codegen-expr
  "Returns a map containing
    * `:return-types` (set)
    * `:continue` (fn).
      Returned fn expects a function taking a single return-type and the emitted code for that return-type.
      May be called multiple times if there are multiple return types.
      Returned fn returns the emitted code for the expression (including the code supplied by the callback)."
  (fn [{:keys [op]} {:keys [var->types]}]
    op))

(defn resolve-string ^String [x]
  (cond
    (instance? ByteBuffer x)
    (str (.decode StandardCharsets/UTF_8 (.duplicate ^ByteBuffer x)))

    (bytes? x)
    (String. ^bytes x StandardCharsets/UTF_8)

    (string? x)
    x))

(defn lit->param [{:keys [op] :as expr}]
  (if (= op :literal)
    (let [{:keys [literal]} expr]
      {:op :param, :param (gensym 'lit),
       :literal literal
       :param-class (class literal)
       :param-type (.arrowType (types/value->leg-type literal))})
    expr))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti get-value-form
  (fn [arrow-type vec-sym idx-sym]
    (class arrow-type))
  :default ::default)

(defmethod get-value-form ArrowType$Null [_ _ _] nil)
(defmethod get-value-form ArrowType$Bool [_ vec-sym idx-sym] `(= 1 (.get ~vec-sym ~idx-sym)))
(defmethod get-value-form ArrowType$FloatingPoint [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Int [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Timestamp [_ vec-sym idx-sym] `(.get ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Duration [_ vec-sym idx-sym] `(DurationVector/get (.getDataBuffer ~vec-sym) ~idx-sym))
(defmethod get-value-form ArrowType$Utf8 [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$Binary [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))
(defmethod get-value-form ArrowType$FixedSizeBinary [_ vec-sym idx-sym] `(util/element->nio-buffer ~vec-sym ~idx-sym))

;; will unify both possible vector representations
;; as an epoch int, do not think it is worth branching for both cases in all date functions.
(defmethod get-value-form ArrowType$Date [^ArrowType$Date type vec-sym idx-sym]
  (util/case-enum (.getUnit type)
    DateUnit/DAY `(.get ~vec-sym ~idx-sym)
    DateUnit/MILLISECOND `(int (quot (.get ~vec-sym ~idx-sym) 86400000))))

;; unifies to nanos (of day) long
(defmethod get-value-form ArrowType$Time [^ArrowType$Time type vec-sym idx-sym]
  (util/case-enum (.getUnit type)
    TimeUnit/NANOSECOND `(.get ~vec-sym ~idx-sym)
    TimeUnit/MICROSECOND `(* 1e3 (.get ~vec-sym ~idx-sym))
    TimeUnit/MILLISECOND `(* 1e6 (long (get ~vec-sym ~idx-sym)))
    TimeUnit/SECOND `(* 1e9 (long (get ~vec-sym ~idx-sym)))))

;; we will box for simplicity given bigger than a long in worst case, may later change to a more primitive
(defmethod get-value-form ArrowType$Interval [_type vec-sym idx-sym]
  `(types/get-object ~vec-sym ~idx-sym))

(defmethod get-value-form ArrowType$ExtensionType
  [^ArrowType$ExtensionType arrow-type vec-sym idx-sym]
  ;; a reasonable default, but implement it for more specific types if this doesn't work
  (get-value-form (.storageType arrow-type) `(.getUnderlyingVector ~vec-sym) idx-sym))

(defn- wrap-mono-return [{:keys [return-types continue]}]
  {:return-types return-types
   :continue (if (= 1 (count return-types))
               ;; only generate the surrounding code once if we're monomorphic
               (fn [f]
                 (f (first return-types)
                    (continue (fn [_ code] code))))

               continue)})

(defmethod codegen-expr :local [{:keys [local]} {:keys [local-types]}]
  (let [return-type (get local-types local)]
    {:return-types #{return-type}
     :continue (fn [f] (f return-type local))}))

(defmethod codegen-expr :let [{:keys [local expr body]} opts]
  (let [{continue-expr :continue, :as emitted-expr} (codegen-expr expr opts)
        emitted-bodies (->> (for [local-type (:return-types emitted-expr)]
                              (MapEntry/create local-type
                                               (codegen-expr body (assoc-in opts [:local-types local] local-type))))
                            (into {}))
        return-types (into #{} (mapcat :return-types) (vals emitted-bodies))]
    (-> {:return-types return-types
         :continue (fn [f]
                     (continue-expr (fn [local-type code]
                                      `(let [~local ~code]
                                         ~((:continue (get emitted-bodies local-type)) f)))))}
        (wrap-mono-return))))

(defmethod codegen-expr :if-some [{:keys [local expr then else]} opts]
  (let [{continue-expr :continue, expr-rets :return-types} (codegen-expr expr opts)
        emitted-thens (->> (for [local-type (-> expr-rets
                                                (disj types/null-type))]
                             (MapEntry/create local-type
                                              (codegen-expr then (assoc-in opts [:local-types local] local-type))))
                           (into {}))
        then-rets (into #{} (mapcat :return-types) (vals emitted-thens))]

    (-> (if-not (contains? expr-rets types/null-type)
          {:return-types then-rets
           :continue (fn [f]
                       (continue-expr (fn [local-type code]
                                        `(let [~local ~code]
                                           ~((:continue (get emitted-thens local-type)) f)))))}

          (let [{e-rets :return-types, e-cont :continue} (codegen-expr else opts)
                return-types (into then-rets e-rets)]
            {:return-types return-types
             :continue (fn [f]
                         (continue-expr (fn [local-type code]
                                          (if (= local-type types/null-type)
                                            (e-cont f)
                                            `(let [~local ~code]
                                               ~((:continue (get emitted-thens local-type)) f))))))}))
        (wrap-mono-return))))

(defmulti codegen-call
  (fn [{:keys [f arg-types]}]
    (vec (cons (keyword (name f)) (map class arg-types))))
  :hierarchy #'types/arrow-type-hierarchy)

(def call-returns-null
  {:return-type types/null-type
   :code nil})

(doseq [[f-kw cmp] [[:< #(do `(neg? ~%))]
                    [:<= #(do `(not (pos? ~%)))]
                    [:> #(do `(pos? ~%))]
                    [:>= #(do `(not (neg? ~%)))]]
        :let [f-sym (symbol (name f-kw))]]

  (doseq [arrow-type #{::types/Number ArrowType$Timestamp ArrowType$Duration ArrowType$Date}]
    (defmethod codegen-call [f-kw arrow-type arrow-type] [{:keys [emitted-args]}]
      {:return-type types/bool-type
       :code `(~f-sym ~@emitted-args)}))

  (doseq [arrow-type #{ArrowType$Binary ArrowType$Utf8}]
    (defmethod codegen-call [f-kw arrow-type arrow-type] [{:keys [emitted-args]}]
      {:return-type types/bool-type
       :code (cmp `(util/compare-nio-buffers-unsigned ~@emitted-args))}))

  (defmethod codegen-call [f-kw ::types/Object ::types/Object] [{:keys [emitted-args]}]
    {:return-type types/bool-type
     :code (cmp `(compare ~@emitted-args))}))

(defmethod codegen-call [:= ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:return-type types/bool-type
   :code `(== ~@emitted-args)})

(defmethod codegen-call [:= ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:return-type types/bool-type
   :code `(= ~@emitted-args)})

;; null-eq is an internal function used in situations where two nulls should compare equal,
;; e.g when grouping rows in group-by.
(defmethod codegen-call [:null-eq ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:return-type types/bool-type
   :code `(= ~@emitted-args)})

(defmethod codegen-call [:null-eq ArrowType$Null ::types/Object] [_]
  {:return-type types/bool-type
   :code false})

(defmethod codegen-call [:null-eq ::types/Object ArrowType$Null] [_]
  {:return-type types/bool-type
   :code false})

(defmethod codegen-call [:null-eq ArrowType$Null ArrowType$Null] [_]
  {:return-type types/bool-type
   :code true})

(defmethod codegen-call [:<> ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:return-type types/bool-type
   :code `(not (== ~@emitted-args))})

(defmethod codegen-call [:<> ::types/Object ::types/Object] [{:keys [emitted-args]}]
  {:return-type types/bool-type
   :code `(not= ~@emitted-args)})

(doseq [f-kw #{:= :< :<= :> :>= :<>}]
  (defmethod codegen-call [f-kw ::types/Object ArrowType$Null] [_] call-returns-null)
  (defmethod codegen-call [f-kw ArrowType$Null ::types/Object] [_] call-returns-null)
  (defmethod codegen-call [f-kw ArrowType$Null ArrowType$Null] [_] call-returns-null))

(defmethod codegen-call [:and ArrowType$Bool ArrowType$Bool] [_]
  (throw (UnsupportedOperationException.))
  #_
  {:return-type types/bool-type
   :code #(do `(and ~@%))})

(defmethod codegen-call [:and ArrowType$Bool ArrowType$Null] [_]
  (throw (UnsupportedOperationException.))
  #_
  {:return-types #{types/null-type types/bool-type}
   :continue-call (fn [f [x-code _]]
                    `(if-not ~x-code
                       ~(f types/bool-type false)
                       ~(f types/null-type nil)))})

(defmethod codegen-call [:and ArrowType$Null ArrowType$Bool] [_]
  (throw (UnsupportedOperationException.))
  #_
  {:return-types #{types/null-type types/bool-type}
   :continue-call (fn [f [_ y-code]]
                    `(if-not ~y-code
                       ~(f types/bool-type false)
                       ~(f types/null-type nil)))})

(defmethod codegen-call [:and ArrowType$Null ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:or ArrowType$Bool ArrowType$Bool] [_]
  (throw (UnsupportedOperationException.))
  #_
  {:return-type types/bool-type
   :code #(do `(or ~@%))})

(defmethod codegen-call [:or ArrowType$Bool ArrowType$Null] [_]
  (throw (UnsupportedOperationException.))
  #_
  {:return-types #{types/null-type types/bool-type}
   :continue-call (fn [f [x-code _]]
                    `(if ~x-code
                       ~(f types/bool-type true)
                       ~(f types/null-type nil)))})

(defmethod codegen-call [:or ArrowType$Null ArrowType$Bool] [_]
  (throw (UnsupportedOperationException.))
  #_
  {:return-types #{types/null-type types/bool-type}
   :continue-call (fn [f [_ y-code]]
                    `(if ~y-code
                       ~(f types/bool-type true)
                       ~(f types/null-type nil)))})

(defmethod codegen-call [:or ArrowType$Null ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:not ArrowType$Bool] [{:keys [emitted-args]}]
  {:return-type types/bool-type
   :code `(not ~@emitted-args)})

(defmethod codegen-call [:not ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:true? ArrowType$Bool] [{:keys [emitted-args]}]
  {:return-type types/bool-type
   :code `(true? ~@emitted-args)})

(defmethod codegen-call [:false? ArrowType$Bool] [{:keys [emitted-args]}]
  {:return-type types/bool-type
   :code `(false? ~@emitted-args)})

(defmethod codegen-call [:nil? ArrowType$Null] [_]
  {:return-type types/bool-type
   :code true})

(doseq [f #{:true? :false? :nil?}]
  (defmethod codegen-call [f ::types/Object] [_]
    {:return-type types/bool-type
     :code false}))

(defmethod codegen-call [:boolean ArrowType$Null] [_]
  {:return-type types/bool-type
   :code false})

(defmethod codegen-call [:boolean ArrowType$Bool] [{[v] :emitted-args}]
  {:return-type types/bool-type
   :code v})

(defmethod codegen-call [:boolean ::types/Object] [_]
  {:return-type types/bool-type
   :code true})

(defn- with-math-integer-cast
  "java.lang.Math's functions only take int or long, so we introduce an up-cast if need be"
  [^ArrowType$Int arrow-type emitted-args]
  (let [arg-cast (if (= 64 (.getBitWidth arrow-type)) 'long 'int)]
    (map #(list arg-cast %) emitted-args)))

(defmethod codegen-call [:+ ArrowType$Int ArrowType$Int] [{:keys [arg-types emitted-args]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :code (list (type->cast return-type)
                 `(Math/addExact ~@(with-math-integer-cast return-type emitted-args)))}))

(defmethod codegen-call [:+ ::types/Number ::types/Number] [{:keys [arg-types emitted-args]}]
  {:return-type (types/least-upper-bound arg-types)
   :code `(+ ~@emitted-args)})

(defmethod codegen-call [:- ArrowType$Int ArrowType$Int] [{:keys [arg-types emitted-args]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)]
    {:return-type return-type
     :code (list (type->cast return-type)
                 `(Math/subtractExact ~@(with-math-integer-cast return-type emitted-args)))}))

(defmethod codegen-call [:- ::types/Number ::types/Number] [{:keys [arg-types emitted-args]}]
  {:return-type (types/least-upper-bound arg-types)
   :code `(- ~@emitted-args)})

(defmethod codegen-call [:- ArrowType$Int] [{[^ArrowType$Int x-type] :arg-types, :keys [emitted-args]}]
  {:return-type x-type
   :code (list (type->cast x-type)
               `(Math/negateExact ~@(with-math-integer-cast x-type emitted-args)))})

(defmethod codegen-call [:- ::types/Number] [{[x-type] :arg-types, :keys [emitted-args]}]
  {:return-type x-type
   :code `(- ~@emitted-args)})

(defmethod codegen-call [:- ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:* ArrowType$Int ArrowType$Int] [{:keys [arg-types emitted-args]}]
  (let [^ArrowType$Int return-type (types/least-upper-bound arg-types)
        arg-cast (if (= 64 (.getBitWidth return-type)) 'long 'int)]
    {:return-type return-type
     :code (list (type->cast return-type)
                 `(Math/multiplyExact ~@(map #(list arg-cast %) emitted-args)))}))

(defmethod codegen-call [:* ::types/Number ::types/Number] [{:keys [arg-types emitted-args]}]
  {:return-type (types/least-upper-bound arg-types)
   :code `(* ~@emitted-args)})

(defmethod codegen-call [:mod ::types/Number ::types/Number] [{:keys [arg-types emitted-args]}]
  {:return-type (types/least-upper-bound arg-types)
   :code `(mod ~@emitted-args)})

(defmethod codegen-call [:/ ArrowType$Int ArrowType$Int] [{:keys [arg-types emitted-args]}]
  {:return-type (types/least-upper-bound arg-types)
   :code `(quot ~@emitted-args)})

(defmethod codegen-call [:/ ::types/Number ::types/Number] [{:keys [arg-types emitted-args]}]
  {:return-type (types/least-upper-bound arg-types)
   :code `(/ ~@emitted-args)})

(defmethod codegen-call [:max ::types/Number ::types/Number] [{:keys [arg-types emitted-args]}]
  {:return-type (types/least-upper-bound arg-types)
   :code `(Math/max ~@emitted-args)})

(defmethod codegen-call [:min ::types/Number ::types/Number] [{:keys [arg-types emitted-args]}]
  {:return-type (types/least-upper-bound arg-types)
   :code `(Math/min ~@emitted-args)})

(defmethod codegen-call [:power ::types/Number ::types/Number] [{:keys [emitted-args]}]
  {:return-type types/float8-type
   :code `(Math/pow ~@emitted-args)})

(defmethod codegen-call [:log ::types/Number ::types/Number] [{[base x] :emitted-args}]
  {:return-type types/float8-type
   :code `(/ (Math/log ~x) (Math/log ~base))})

(doseq [f #{:+ :- :* :/ :mod :min :max :power :log}]
  (defmethod codegen-call [f ::types/Number ArrowType$Null] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ::types/Number] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ArrowType$Null] [_] call-returns-null))

(defmethod codegen-call [:double ::types/Number] [{:keys [emitted-args]}]
  {:return-type types/float8-type
   :code `(double ~@emitted-args)})

(defmethod codegen-call [:double ArrowType$Null] [_] call-returns-null)

;; TODO extend min/max to non-numeric - numeric handled by `java.lang.Math`, below
(doseq [f #{:min :max}]
  (defmethod codegen-call [f ::types/Number ArrowType$Null] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ::types/Number] [_] call-returns-null)
  (defmethod codegen-call [f ArrowType$Null ArrowType$Null] [_] call-returns-null))

(defn like->regex [like-pattern]
  (-> like-pattern
      (Pattern/quote)
      (.replace "%" "\\E.*\\Q")
      (.replace "_" "\\E.\\Q")
      (->> (format "^%s$"))
      re-pattern))

(defmethod codegen-call [:like ArrowType$Utf8 ArrowType$Utf8] [{[_ {:keys [literal]}] :args, [haystack-code needle-code] :emitted-args}]
  {:return-type types/bool-type
   :code `(boolean (re-find ~(if literal
                               (like->regex literal)
                               `(like->regex (resolve-string ~needle-code)))
                            (resolve-string ~haystack-code)))})

(defmethod codegen-call [:like ArrowType$Utf8 ArrowType$Null] [_] call-returns-null)
(defmethod codegen-call [:like ArrowType$Null ArrowType$Utf8] [_] call-returns-null)
(defmethod codegen-call [:like ArrowType$Null ArrowType$Null] [_] call-returns-null)

(defn binary->hex-like-pattern
  "Returns a like pattern that will match on binary encoded to hex.

  Percent/underscore are encoded as the ascii/utf-8 binary value for their respective characters i.e

  The byte 95 should be used for _
  The byte 37 should be used for %.

  Useful for now to use the string-impl of LIKE for binary search too."
  [^bytes bin]
  (-> (Hex/encodeHexString bin)
      (.replace "25" "%")
      ;; we are matching two chars per byte (hex)
      (.replace "5f" "__")))

(defn buf->bytes
  "Returns a byte array given an nio buffer. Gives you the backing array if possible, else allocates a new array."
  ^bytes [^ByteBuffer o]
  (if (.hasArray o)
    (.array o)
    (let [size (.remaining o)
          arr (byte-array size)]
      (.get o arr)
      arr)))

(defn resolve-bytes
  "Values of type ArrowType$Binary may have differing literal representations, e.g a byte array, or NIO byte buffer.

  Use this function when you are unsure of the representation at hand and just want a byte array."
  ^bytes [binary]
  (if (bytes? binary)
    binary
    (buf->bytes binary)))

(defn resolve-buf
  "Values of type ArrowType$Binary may have differing literal representations, e.g a byte array, or NIO byte buffer.

  Use this function when you are unsure of the representation at hand and just want a ByteBuffer."
  ^ByteBuffer [buf-or-bytes]
  (if (instance? ByteBuffer buf-or-bytes)
    buf-or-bytes
    (ByteBuffer/wrap ^bytes buf-or-bytes)))

(defmethod codegen-call [:like ArrowType$Binary ArrowType$Binary] [{[_ {needle-literal :literal}] :args,
                                                                    [haystack-code needle-code] :emitted-args}]
  {:return-type types/bool-type
   :code `(boolean (re-find ~(if needle-literal
                               (like->regex (binary->hex-like-pattern (resolve-bytes needle-literal)))
                               `(like->regex (binary->hex-like-pattern (buf->bytes ~needle-code))))
                            (Hex/encodeHexString (buf->bytes ~haystack-code))))})

(defmethod codegen-call [:like ArrowType$Binary ArrowType$Null] [_] call-returns-null)
(defmethod codegen-call [:like ArrowType$Null ArrowType$Binary] [_] call-returns-null)

(defmethod codegen-call [:like-regex ArrowType$Utf8 ArrowType$Utf8 ArrowType$Utf8]
  [{:keys [args], [haystack-code needle-code] :emitted-args}]
  (let [[_ re-literal flags] (map :literal args)

        flag-map
        {\s [Pattern/DOTALL]
         \i [Pattern/CASE_INSENSITIVE, Pattern/UNICODE_CASE]
         \m [Pattern/MULTILINE]}

        flag-int (int (reduce bit-or 0 (mapcat flag-map flags)))]

    {:return-type types/bool-type
     :code `(boolean (re-find ~(if re-literal
                                 (Pattern/compile re-literal flag-int)
                                 `(Pattern/compile (resolve-string ~needle-code) ~flag-int))
                              (resolve-string ~haystack-code)))}))

(defmethod codegen-call [:like-regex ArrowType$Utf8 ArrowType$Null ArrowType$Utf8] [_] call-returns-null)
(defmethod codegen-call [:like-regex ArrowType$Null ArrowType$Utf8 ArrowType$Utf8] [_] call-returns-null)
(defmethod codegen-call [:like-regex ArrowType$Null ArrowType$Null ArrowType$Utf8] [_] call-returns-null)

;; apache commons has these functions but did not think they were worth the dep.
;; replace if we ever put commons on classpath.

(defn- sql-trim-leading
  [^String s ^String trim-char]
  (let [trim-cp (.codePointAt trim-char 0)]
    (loop [i 0]
      (if (< i (.length s))
        (let [cp (.codePointAt s i)]
          (if (= cp trim-cp)
            (recur (unchecked-add-int i (Character/charCount cp)))
            (.substring s i (.length s))))
        ""))))

(defn- sql-trim-trailing
  [^String s ^String trim-char]
  (let [trim-cp (.codePointAt trim-char 0)]
    (loop [i (unchecked-dec-int (.length s))
           len (.length s)]
      (if (< -1 i)
        (let [cp (.codePointAt s i)]
          (if (= cp trim-cp)
            (recur (unchecked-subtract-int i (Character/charCount cp))
                   (unchecked-subtract-int len (Character/charCount cp)))
            (.substring s 0 len)))
        ""))))

(defn- trim-char-conforms?
  "Trim char is allowed to be length 1, or 2 - but only 2 if the 2 chars represent a single unicode character
  (but are split due to utf-16 surrogacy)."
  [^String trim-char]
  (or (= 1 (.length trim-char))
      (and (= 2 (.length trim-char))
           (= 2 (Character/charCount (.codePointAt trim-char 0))))))

(defn sql-trim
  "SQL Trim function.

  trim-spec is a string having one of the values: BOTH | TRAILING | LEADING.

  trim-char is a **SINGLE** unicode-character string e.g \" \". The string can include 2-char code points, as long as it is one
  logical unicode character.

  N.B trim-char is currently restricted to just one character, not all database implementations behave this way, in postgres you
  can specify multi-character strings for the trim char.

  See also sql-trim-leading, sql-trim-trailing"
  ^String [^String s ^String trim-spec ^String trim-char]
  (when-not (trim-char-conforms? trim-char) (throw (IllegalArgumentException. "Data Exception - trim error.")))
  (case trim-spec
    "LEADING" (sql-trim-leading s trim-char)
    "TRAILING" (sql-trim-trailing s trim-char)
    "BOTH" (-> s (sql-trim-leading trim-char) (sql-trim-trailing trim-char))))

(defmethod codegen-call [:trim ArrowType$Utf8 ArrowType$Utf8 ArrowType$Utf8] [{[s trim-spec trim-char] :emitted-args}]
  {:return-type types/varchar-type
   :code `(ByteBuffer/wrap (.getBytes (sql-trim (resolve-string ~s) (resolve-string ~trim-spec) (resolve-string ~trim-char))
                                      StandardCharsets/UTF_8))})

(defmethod codegen-call [:trim ArrowType$Null ArrowType$Utf8 ArrowType$Utf8] [_] call-returns-null)

(defmethod codegen-call [:trim ArrowType$Null ArrowType$Utf8 ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:trim ArrowType$Utf8 ArrowType$Utf8 ArrowType$Null] [_] call-returns-null)

(defn- binary-trim-leading
  [^bytes bin trim-octet]
  (let [trim-octet (byte trim-octet)]
    (loop [i 0]
      (if (< i (alength bin))
        (let [v (aget bin i)]
          (if (= v trim-octet)
            (recur (unchecked-inc-int i))
            (if (zero? i)
              bin
              (Arrays/copyOfRange bin i (alength bin)))))
        (byte-array 0)))))

(defn- binary-trim-trailing
  [^bytes bin trim-octet]
  (let [trim-octet (byte trim-octet)]
    (loop [i (unchecked-dec-int (alength bin))
           len (alength bin)]
      (if (< -1 i)
        (let [v (aget bin i)]
          (if (= v trim-octet)
            (recur (unchecked-dec-int i) (unchecked-dec-int len))
            (if (= len (alength bin))
              bin
              (Arrays/copyOfRange bin 0 len))))
        (byte-array 0)))))

(defn binary-trim
  "SQL Trim function on binary.

  trim-spec is a string having one of the values: BOTH | TRAILING | LEADING.

  trim-octet is the byte value to trim.

  See also binary-trim-leading, binary-trim-trailing"
  [^bytes bin ^String trim-spec trim-octet]
  (case trim-spec
    "BOTH" (-> bin (binary-trim-leading trim-octet) (binary-trim-trailing trim-octet))
    "LEADING" (binary-trim-leading bin trim-octet)
    "TRAILING" (binary-trim-trailing bin trim-octet)))

(defmethod codegen-call [:trim ArrowType$Binary ArrowType$Utf8 ArrowType$Binary] [{[s trim-spec trim-octet] :emitted-args}]
  {:return-type types/varbinary-type
   :code `(ByteBuffer/wrap (binary-trim (resolve-bytes ~s) (resolve-string ~trim-spec) (first (resolve-bytes ~trim-octet))))})

(defmethod codegen-call [:trim ArrowType$Null ArrowType$Utf8 ArrowType$Binary] [_] call-returns-null)

(defmethod codegen-call [:trim ArrowType$Null ArrowType$Utf8 ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:trim ArrowType$Binary ArrowType$Utf8 ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:trim ArrowType$Binary ArrowType$Utf8 ::types/Number] [{[s trim-spec trim-octet] :emitted-args}]
  {:return-type types/varbinary-type
   ;; should we throw an explicit error if no good cast to byte is possible?
   :code `(ByteBuffer/wrap (binary-trim (resolve-bytes ~s) (resolve-string ~trim-spec) (byte ~trim-octet)))})

(defmethod codegen-call [:trim ArrowType$Null ArrowType$Utf8 ::types/Number] [_] call-returns-null)

(defmethod codegen-call [:upper ArrowType$Utf8] [_]
  {:return-type
   types/varchar-type
   :code
   (fn [[code]]
     `(ByteBuffer/wrap (.getBytes (.toUpperCase (resolve-string ~code)) StandardCharsets/UTF_8)))})

(defmethod codegen-call [:upper ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:lower ArrowType$Utf8] [{[arg] :emitted-args}]
  {:return-type types/varchar-type
   :code `(ByteBuffer/wrap (.getBytes (.toLowerCase (resolve-string ~arg)) StandardCharsets/UTF_8))})

(defmethod codegen-call [:lower ArrowType$Null] [_] call-returns-null)

;; todo high perf variadic version, or specialisation for arity up to N.
(defmethod codegen-call [:concat ArrowType$Utf8 ArrowType$Utf8] [_]
  {:return-type types/varchar-type
   :code (fn [[a b]]
           `(ByteBuffer/wrap (.getBytes (str (resolve-string ~a) (resolve-string ~b))
                                        StandardCharsets/UTF_8)))})

(defmethod codegen-call [:concat ArrowType$Null ArrowType$Null] [_] call-returns-null)
(defmethod codegen-call [:concat ArrowType$Utf8 ArrowType$Null] [_] call-returns-null)
(defmethod codegen-call [:concat ArrowType$Null ArrowType$Utf8] [_] call-returns-null)

(defn bconcat ^bytes [^bytes b1 ^bytes b2]
  (let [ret (byte-array (+ (alength b1) (alength b2)))]
    (System/arraycopy b1 0 ret 0 (alength b1))
    (System/arraycopy b2 0 ret (alength b1) (alength b2))
    ret))

(defmethod codegen-call [:concat ArrowType$Binary ArrowType$Binary] [{[a b] :emitted-args}]
  {:return-type types/varbinary-type
   :code `(ByteBuffer/wrap (bconcat (resolve-bytes ~a) (resolve-bytes ~b)))})

(defmethod codegen-call [:concat ArrowType$Binary ArrowType$Null] [_] call-returns-null)
(defmethod codegen-call [:concat ArrowType$Null ArrowType$Binary] [_] call-returns-null)

(defn resolve-utf8-buf ^ByteBuffer [s-or-buf]
  (if (instance? ByteBuffer s-or-buf)
    s-or-buf
    (ByteBuffer/wrap (.getBytes ^String s-or-buf StandardCharsets/UTF_8))))

(defmethod codegen-call [:substring ArrowType$Utf8 ArrowType$Int ArrowType$Int ArrowType$Bool] [{[x start length use-len] :emitted-args}]
  {:return-type types/varchar-type
   :code `(StringUtil/sqlUtf8Substring (resolve-utf8-buf ~x) ~start ~length ~use-len)})

(defmethod codegen-call [:substring ArrowType$Binary ArrowType$Int ArrowType$Int ArrowType$Bool] [{[x start length use-len] :emitted-args}]
  {:return-type types/varbinary-type
   :code `(StringUtil/sqlBinSubstring (resolve-buf ~x) ~start ~length ~use-len)})

;; nil specialisation for substring
(doseq [a [ArrowType$Utf8 ArrowType$Binary ArrowType$Null]
        b [ArrowType$Int ArrowType$Null]
        c [ArrowType$Int ArrowType$Null]
        :when (some #(= ArrowType$Null %) [a b c])]
  (defmethod codegen-call [:substring a b c ArrowType$Bool] [_] call-returns-null))

(defmethod codegen-call [:character-length ArrowType$Utf8 ArrowType$Utf8] [{:keys [args], [s-code _unit-code] :emitted-args}]
  (let [[_ unit] (map :literal args)]
    {:return-type types/int-type
     :code (case unit
             "CHARACTERS" `(StringUtil/utf8Length ~s-code)
             "OCTETS" `(.remaining ~(-> s-code (with-tag ByteBuffer))))}))

(defmethod codegen-call [:character-length ArrowType$Null ArrowType$Utf8] [_] call-returns-null)

(defmethod codegen-call [:octet-length ArrowType$Utf8] [{[buf-code] :emitted-args}]
  {:return-type types/int-type
   :code `(.remaining ~(-> buf-code (with-tag ByteBuffer)))})

(defmethod codegen-call [:octet-length ArrowType$Binary] [{[buf-code] :emitted-args}]
  {:return-type types/int-type
   :code `(.remaining ~(-> buf-code (with-tag ByteBuffer)))})

(defmethod codegen-call [:octet-length ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:position ArrowType$Utf8 ArrowType$Utf8 ArrowType$Utf8] [{[needle haystack _unit] :emitted-args, :keys [args]}]
  {:return-type types/int-type
   :code (case (:literal (last args))
           "CHARACTERS" `(StringUtil/sqlUtf8Position (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack))
           "OCTETS" `(StringUtil/SqlBinPosition (resolve-utf8-buf ~needle) (resolve-utf8-buf ~haystack)))})

(defmethod codegen-call [:position ArrowType$Utf8 ArrowType$Null ArrowType$Utf8] [_] call-returns-null)
(defmethod codegen-call [:position ArrowType$Null ArrowType$Utf8 ArrowType$Utf8] [_] call-returns-null)
(defmethod codegen-call [:position ArrowType$Null ArrowType$Null ArrowType$Utf8] [_] call-returns-null)

(defmethod codegen-call [:position ArrowType$Binary ArrowType$Binary] [_]
  {:return-type types/int-type
   :code (fn [[needle haystack]] `(StringUtil/SqlBinPosition (resolve-buf ~needle) (resolve-buf ~haystack)))})

(defmethod codegen-call [:position ArrowType$Binary ArrowType$Null] [_] call-returns-null)
(defmethod codegen-call [:position ArrowType$Null ArrowType$Binary] [_] call-returns-null)
(defmethod codegen-call [:position ArrowType$Null ArrowType$Null] [_] call-returns-null)

;; instantiate defaults for overlay, 4 nilable args is too much to list!
(doseq [target [ArrowType$Utf8 ArrowType$Binary ArrowType$Null]
        replacement [ArrowType$Utf8 ArrowType$Binary ArrowType$Null]
        start [ArrowType$Int ArrowType$Null]
        len [ArrowType$Int ArrowType$Null]
        :when (some #(= ArrowType$Null %) [target replacement start len])]
  (defmethod codegen-call [:overlay target replacement start len] [_] call-returns-null))

(defmethod codegen-call [:overlay ArrowType$Utf8 ArrowType$Utf8 ArrowType$Int ArrowType$Int] [{[target replacement from len] :emitted-args}]
  {:return-type types/varchar-type
   :code `(StringUtil/sqlUtf8Overlay (resolve-utf8-buf ~target) (resolve-utf8-buf ~replacement) ~from ~len)})

(defmethod codegen-call [:overlay ArrowType$Binary ArrowType$Binary ArrowType$Int ArrowType$Int] [{[target replacement from len] :emitted-args}]
  {:return-type types/varbinary-type
   :code `(StringUtil/sqlBinOverlay (resolve-buf ~target) (resolve-buf ~replacement) ~from ~len)})

(defmethod codegen-call [:default-overlay-length ArrowType$Null] [_] call-returns-null)

(defmethod codegen-call [:default-overlay-length ArrowType$Utf8] [{:keys [emitted-args]}]
  {:return-type types/int-type
   :code `(StringUtil/utf8Length (resolve-utf8-buf ~@emitted-args))})

(defmethod codegen-call [:default-overlay-length ArrowType$Binary] [{:keys [emitted-args]}]
  {:return-type types/int-type
   :code `(.remaining (resolve-buf ~@emitted-args))})

(defmethod codegen-call [:extract ArrowType$Utf8 ArrowType$Timestamp] [{[{field :literal} _] :args, [_ ts-code] :emitted-args}]
  {:return-type types/bigint-type
   :code `(.get (.atOffset ^Instant (util/micros->instant ~ts-code) ZoneOffset/UTC)
                ~(case field
                   "YEAR" `ChronoField/YEAR
                   "MONTH" `ChronoField/MONTH_OF_YEAR
                   "DAY" `ChronoField/DAY_OF_MONTH
                   "HOUR" `ChronoField/HOUR_OF_DAY
                   "MINUTE" `ChronoField/MINUTE_OF_HOUR))})

(defmethod codegen-call [:extract ArrowType$Utf8 ArrowType$Date] [{[{field :literal} _] :args, [_ epoch-day-code] :emitted-args}]
  {:return-type types/int-type
   :code (case field
           ;; we could inline the math here, but looking at sources, there is some nuance.
           "YEAR" `(.getYear (LocalDate/ofEpochDay ~epoch-day-code))
           "MONTH" `(.getMonthValue (LocalDate/ofEpochDay ~epoch-day-code))
           "DAY" `(.getDayOfMonth (LocalDate/ofEpochDay ~epoch-day-code))
           "HOUR" 0
           "MINUTE" 0)})

(defmethod codegen-call [:date-trunc ArrowType$Utf8 ArrowType$Timestamp] [{[{field :literal} _] :args, [_ date-type] :arg-types, [_ x] :emitted-args}]
  {:return-type date-type
   :code `(util/instant->micros (-> (util/micros->instant ~x)
                                    ;; TODO only need to create the ZoneId once per batch
                                    ;; but getting calls to have batch-bindings isn't straightforward
                                    ;; NOTE might be more straightforward now the IoC's gone
                                    (ZonedDateTime/ofInstant (ZoneId/of ~(.getTimezone ^ArrowType$Timestamp date-type)))
                                    ~(case field
                                       "YEAR" `(-> (.truncatedTo ChronoUnit/DAYS) (.withDayOfYear 1))
                                       "MONTH" `(-> (.truncatedTo ChronoUnit/DAYS) (.withDayOfMonth 1))
                                       `(.truncatedTo ~(case field
                                                         "DAY" `ChronoUnit/DAYS
                                                         "HOUR" `ChronoUnit/HOURS
                                                         "MINUTE" `ChronoUnit/MINUTES
                                                         "SECOND" `ChronoUnit/SECONDS
                                                         "MILLISECOND" `ChronoUnit/MILLIS
                                                         "MICROSECOND" `ChronoUnit/MICROS)))
                                    (.toInstant)))})

(defmethod codegen-call [:date-trunc ArrowType$Utf8 ArrowType$Date] [{[{field :literal} _] :args, [_ epoch-day-code] :emitted-args}]
  {:return-type types/date-day-type
   :code (case field
           "YEAR" `(-> ~epoch-day-code LocalDate/ofEpochDay (.withDayOfYear 1) .toEpochDay)
           "MONTH" `(-> ~epoch-day-code LocalDate/ofEpochDay (.withDayOfMonth 1) .toEpochDay)
           "DAY" epoch-day-code
           "HOUR" epoch-day-code
           "MINUTE" epoch-day-code)})

(def ^:dynamic ^java.time.Clock *clock* (Clock/systemDefaultZone))

(defn- bound-precision ^long [^long precision]
  (-> precision (max 0) (min 9)))

(def ^:private precision-timeunits
  [TimeUnit/SECOND
   TimeUnit/MILLISECOND TimeUnit/MILLISECOND TimeUnit/MILLISECOND
   TimeUnit/MICROSECOND TimeUnit/MICROSECOND TimeUnit/MICROSECOND
   TimeUnit/NANOSECOND TimeUnit/NANOSECOND TimeUnit/NANOSECOND])

(def ^:private seconds-multiplier (mapv long [1e0 1e3 1e3 1e3 1e6 1e6 1e6 1e9 1e9 1e9]))
(def ^:private nanos-divisor (mapv long [1e9 1e6 1e6 1e6 1e3 1e3 1e3 1e0 1e0 1e0]))
(def ^:private precision-modulus (mapv long [1e0 1e2 1e1 1e0 1e2 1e1 1e0 1e2 1e1 1e0]))

(defn- truncate-for-precision [code precision]
  (let [^long modulus (precision-modulus precision)]
    (if (= modulus 1)
      code
      `(* ~modulus (quot ~code ~modulus)))))

(defn- current-timestamp [^long precision]
  (let [precision (bound-precision precision)
        zone-id (.getZone *clock*)]
    {:return-type (ArrowType$Timestamp. (precision-timeunits precision) (str zone-id))
     :code (-> `(long (let [inst# (.instant *clock*)]
                        (+ (* ~(seconds-multiplier precision) (.getEpochSecond inst#))
                           (quot (.getNano inst#) ~(nanos-divisor precision)))))
               (truncate-for-precision precision))}))

(def ^:private default-time-precision 6)

(defmethod codegen-call [:current-timestamp] [_]
  (current-timestamp default-time-precision))

(defmethod codegen-call [:current-timestamp ArrowType$Int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-timestamp precision))

(defmethod codegen-call [:current-date] [_]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Dates to be in UTC.
  ;; we then turn DateDays into LocalDates, which confuses things further.
  {:return-type types/date-day-type
   :code `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) ZoneOffset/UTC)
                    (.toLocalDate)
                    (.toEpochDay)))})

(def timeunit->bitwidth
  {TimeUnit/SECOND 32
   TimeUnit/MILLISECOND 32
   TimeUnit/MICROSECOND 64
   TimeUnit/NANOSECOND 64})

(defn- current-time [^long precision]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Times to be in UTC.
  ;; we then turn times into LocalTimes, which confuses things further.
  (let [precision (bound-precision precision)
        time-unit (precision-timeunits precision)]
    {:return-type (ArrowType$Time. time-unit (timeunit->bitwidth time-unit))
     :code (-> `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) ZoneOffset/UTC)
                          (.toLocalTime)
                          (.toNanoOfDay)
                          (quot ~(nanos-divisor precision))))
               (truncate-for-precision precision))}))

(defmethod codegen-call [:current-time] [_]
  (current-time default-time-precision))

(defmethod codegen-call [:current-time ArrowType$Int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-time precision))

(defn- local-timestamp [^long precision]
  (let [precision (bound-precision precision)]
    {:return-type (ArrowType$Timestamp. (precision-timeunits precision) nil)
     :code (-> `(long (let [ldt# (-> (ZonedDateTime/ofInstant (.instant *clock*) (.getZone *clock*))
                                     (.toLocalDateTime))]
                        (+ (* (.toEpochSecond ldt# ZoneOffset/UTC) ~(seconds-multiplier precision))
                           (quot (.getNano ldt#) ~(nanos-divisor precision)))))
               (truncate-for-precision precision))}))

(defmethod codegen-call [:local-timestamp] [_]
  (local-timestamp default-time-precision))

(defmethod codegen-call [:local-timestamp ArrowType$Int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-timestamp precision))

(defn- local-time [^long precision]
  (let [precision (bound-precision precision)
        time-unit (precision-timeunits precision)]
    {:return-type (ArrowType$Time. time-unit (timeunit->bitwidth time-unit))
     :code (-> `(long (-> (ZonedDateTime/ofInstant (.instant *clock*) (.getZone *clock*))
                          (.toLocalTime)
                          (.toNanoOfDay)
                          (quot ~(nanos-divisor precision))))
               (truncate-for-precision precision))}))

(defmethod codegen-call [:local-time] [_]
  (local-time default-time-precision))

(defmethod codegen-call [:local-time ArrowType$Int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-time precision))

(defmethod codegen-call [:abs ::types/Number] [{[numeric-type] :arg-types, :keys [emitted-args]}]
  {:return-type numeric-type
   :code `(Math/abs ~@emitted-args)})

(defmethod codegen-call [:abs ArrowType$Null] [_] call-returns-null)

(doseq [[math-op math-method] {:sin 'Math/sin
                               :cos 'Math/cos
                               :tan 'Math/tan
                               :sinh 'Math/sinh
                               :cosh 'Math/cosh
                               :tanh 'Math/tanh
                               :asin 'Math/asin
                               :acos 'Math/acos
                               :atan 'Math/atan
                               :sqrt 'Math/sqrt
                               :ln 'Math/log
                               :log10 'Math/log10
                               :exp 'Math/exp
                               :floor 'Math/floor
                               :ceil 'Math/ceil}]
  (defmethod codegen-call [math-op ::types/Number] [{:keys [emitted-args]}]
    {:return-type types/float8-type
     :code `(~math-method ~@emitted-args)})

  (defmethod codegen-call [math-op ArrowType$Null] [_] call-returns-null))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti set-value-form
  (fn [arrow-type out-vec-sym idx-sym code]
    (class arrow-type)))

(defmethod set-value-form ArrowType$Null [_ _out-vec-sym _idx-sym _code])

(defmethod set-value-form ArrowType$Bool [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (if ~code 1 0)))

(defmethod set-value-form ArrowType$Int [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$FloatingPoint [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym (double ~code)))

(defn- set-bytebuf-form [out-vec-sym idx-sym code]
  `(let [^ByteBuffer buf# ~code
         pos# (.position buf#)]
     (.setSafe ~out-vec-sym ~idx-sym buf#
               pos# (.remaining buf#))
     ;; setSafe mutates the buffer's position, so we reset it
     (.position buf# pos#)))

(defmethod set-value-form ArrowType$Utf8 [_ out-vec-sym idx-sym code]
  (set-bytebuf-form out-vec-sym idx-sym code))

(defmethod set-value-form ArrowType$Binary [_ out-vec-sym idx-sym code]
  (set-bytebuf-form out-vec-sym idx-sym code))

(defmethod set-value-form ArrowType$Timestamp [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Duration [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Date [_ out-vec-sym idx-sym code]
  ;; assuming we are writing to a date day vector, as that is the canonical representation
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Time [_ out-vec-sym idx-sym code]
  `(.set ~out-vec-sym ~idx-sym ~code))

(defmethod set-value-form ArrowType$Interval [_ out-vec-sym idx-sym code]
  `(let [period-duration# ~code
         period# (.getPeriod period-duration#)
         duration# (.getDuration period-duration#)]
     (.set ~out-vec-sym ~idx-sym (.toTotalMonths period#) (.getDays period#) (.toNanos duration#))))

(defn batch-bindings [expr]
  (->> (walk/expr-seq expr)
       (mapcat :batch-bindings)
       (distinct)
       (apply concat)))

(defn field->value-types [^Field field]
  ;; potential duplication with LegType
  ;; probably doesn't work with structs, not convinced about lists either.
  (case (.name (Types/getMinorTypeForArrowType (.getType field)))
    "DENSEUNION"
    (mapv #(.getFieldType ^Field %) (.getChildren field))

    (.getFieldType field)))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti emit-expr
  (fn [{:keys [op]} col-name {:keys [var-fields] :as opts}]
    op)
  :default ::default)

(defmethod emit-expr :literal [{:keys [literal]} ^String col-name, _opts]
  (let [arrow-type (.arrowType (types/value->leg-type literal))
        field-type (FieldType. (= arrow-type types/null-type) arrow-type nil)]
    (fn [^IIndirectRelation in-rel, ^BufferAllocator al, _params]
      (let [out-vec (-> field-type
                        (.createNewSingleVector col-name al nil))]
        (try
          (let [out-writer (vw/vec->writer out-vec)
                row-count (.rowCount in-rel)]
            (.setValueCount out-vec row-count)
            (dotimes [_ row-count]
              (.startValue out-writer)
              (types/write-value! literal out-writer)
              (.endValue out-writer))
            out-vec)
          (catch Throwable e
            (.close out-vec)
            (throw e)))))))

(defmethod emit-expr :param [{:keys [param param-type]} ^String col-name, _opts]
  (let [field-type (FieldType. (= param-type types/null-type) param-type nil)]
    (fn [^IIndirectRelation in-rel, ^BufferAllocator al, params]
      (let [out-vec (-> field-type
                        (.createNewSingleVector col-name al nil))
            param-value (get params param)]
        (try
          (let [out-writer (vw/vec->writer out-vec)
                row-count (.rowCount in-rel)]
            (.setValueCount out-vec row-count)
            (dotimes [_ row-count]
              (.startValue out-writer)
              (types/write-value! param-value out-writer)
              (.endValue out-writer))
            out-vec)
          (catch Throwable e
            (.close out-vec)
            (throw e)))))))

(defn field-with-name ^org.apache.arrow.vector.types.pojo.Field [^Field field, ^String col-name]
  (apply types/->field col-name (.getType field) (.isNullable field)
         (.getChildren field)))

(defmethod emit-expr :struct [{:keys [entries]} col-name opts]
  (let [entries (vec (for [[k v] entries]
                       (emit-expr v (name k) opts)))]

    (fn [^IIndirectRelation in-rel, al params]
      (let [evald-entries (LinkedList.)]
        (try
          (doseq [eval-entry entries]
            (.add evald-entries (eval-entry in-rel al params)))

          (let [row-count (.rowCount in-rel)

                ^Field out-field (apply types/->field col-name types/struct-type false
                                        (map #(.getField ^ValueVector %) evald-entries))

                ^StructVector out-vec (.createVector out-field al)]
            (try
              (.setValueCount out-vec row-count)

              (dotimes [idx row-count]
                (.setIndexDefined out-vec idx))

              (doseq [^ValueVector in-child-vec evald-entries
                      :let [out-child-vec (.getChild out-vec (.getName in-child-vec) ValueVector)]]
                (doto (.makeTransferPair in-child-vec out-child-vec)
                  (.splitAndTransfer 0 row-count)))

              out-vec
              (catch Throwable e
                (util/try-close out-vec)
                (throw e))))


          (finally
            (run! util/try-close evald-entries)))))))

(defmethod emit-expr :dot-const-field [{:keys [struct-expr field]} ^String col-name opts]
  (let [eval-expr (emit-expr struct-expr "dot" opts)]
    (fn [in-rel al params]
      (with-open [^FieldVector in-vec (eval-expr in-rel al params)]
        (let [out-field (types/->field col-name types/dense-union-type false)
              out-vec (.createVector out-field al)
              out-writer (vw/vec->writer out-vec)]
          (try
            (.setValueCount out-vec (.getValueCount in-vec))

            (let [col-rdr (-> (.structReader (iv/->direct-vec in-vec))
                              (.readerForKey (name field)))
                  copier (.rowCopier col-rdr out-writer)]

              (dotimes [idx (.getValueCount in-vec)]
                (.startValue out-writer)
                (.copyRow copier idx)
                (.endValue out-writer)))

            out-vec

            (catch Throwable e
              (.close out-vec)
              (throw e))))))))

(defmethod emit-expr :variable [{:keys [variable]} col-name {:keys [var-fields]}]
  (let [^Field out-field (-> (or (get var-fields variable)
                                 (throw (IllegalArgumentException. (str "missing variable: " variable ", available " (pr-str (set (keys var-fields)))))))
                             (field-with-name col-name))]
    (fn [^IIndirectRelation in-rel al _params]
      (let [in-vec (.vectorForName in-rel (name variable))
            out-vec (.createVector out-field al)]
        (.copyTo in-vec out-vec)
        (try
          out-vec
          (catch Throwable e
            (.close out-vec)
            (throw e)))))))

(defmethod emit-expr :list [{:keys [elements]} ^String col-name opts]
  (let [el-count (count elements)
        emitted-els (mapv #(emit-expr % "list-el" opts) elements)]
    (fn [^IIndirectRelation in-rel al params]
      (let [els (LinkedList.)]
        (try
          (doseq [eval-expr emitted-els]
            (.add els (eval-expr in-rel al params)))

          (let [out-vec (-> (types/->field col-name (ArrowType$FixedSizeList. el-count) false
                                           (types/->field "$data" types/dense-union-type false))
                            ^FixedSizeListVector (.createVector al))

                out-writer (.asList (vw/vec->writer out-vec))
                out-data-writer (.getDataWriter out-writer)]
            (try
              (let [child-row-count (.rowCount in-rel)
                    copiers (mapv (fn [res]
                                    (.rowCopier out-data-writer res))
                                  els)]
                (.setValueCount out-vec child-row-count)

                (dotimes [idx child-row-count]
                  (.setNotNull out-vec idx)
                  (.startValue out-writer)

                  (dotimes [el-idx el-count]
                    (.startValue out-data-writer)
                    (doto ^IRowCopier (nth copiers el-idx)
                      (.copyRow idx))
                    (.endValue out-data-writer))

                  (.endValue out-writer)))

              out-vec
              (catch Throwable e
                (.close out-vec)
                (throw e))))

          (finally
            (run! util/try-close els)))))))

(defmethod emit-expr :nth-const-n [{:keys [coll-expr ^long n]} col-name opts]
  (let [eval-coll (emit-expr coll-expr "nth-coll" opts)]
    (fn [in-rel al params]
      (with-open [^FieldVector coll-res (eval-coll in-rel al params)]
        (let [list-rdr (.listReader (iv/->direct-vec coll-res))
              coll-count (.getValueCount coll-res)
              out-vec (-> (types/->field col-name types/dense-union-type false)
                          (.createVector al))]
          (try
            (let [out-writer (vw/vec->writer out-vec)
                  copier (.elementCopier list-rdr out-writer)]

              (.setValueCount out-vec coll-count)

              (dotimes [idx coll-count]
                (.startValue out-writer)
                (.copyElement copier idx n)
                (.endValue out-writer))
              out-vec)
            (catch Throwable e
              (.close out-vec)
              (throw e))))))))

(defn- long-getter ^java.util.function.IntUnaryOperator [n-res]
  (condp = (class n-res)
    IntVector (reify IntUnaryOperator
                (applyAsInt [_ idx]
                  (.get ^IntVector n-res idx)))
    BigIntVector (reify IntUnaryOperator
                   (applyAsInt [_ idx]
                     (int (.get ^BigIntVector n-res idx))))))

(defmethod emit-expr :nth [{:keys [coll-expr n-expr]} col-name opts]
  (let [eval-coll (emit-expr coll-expr "nth-coll" opts)
        eval-n (emit-expr n-expr "nth-n" opts)]
    (fn [in-rel al params]
      (with-open [^FieldVector coll-res (eval-coll in-rel al params)
                  ^FieldVector n-res (eval-n in-rel al params)]
        (let [list-rdr (.listReader (iv/->direct-vec coll-res))
              coll-count (.getValueCount coll-res)
              out-vec (-> (types/->field col-name types/dense-union-type
                                         (or (.isNullable (.getField coll-res))
                                             (.isNullable (.getField n-res))))
                          (.createVector al))]
          (try
            (let [out-writer (vw/vec->writer out-vec)
                  copier (.elementCopier list-rdr out-writer)
                  !null-writer (delay (.writerForType (.asDenseUnion out-writer) LegType/NULL))]

              (.setValueCount out-vec coll-count)

              (let [->n (long-getter n-res)]
                (dotimes [idx coll-count]
                  (.startValue out-writer)
                  (if (.isNull n-res idx)
                    (doto ^IVectorWriter @!null-writer
                      (.startValue)
                      (.endValue))
                    (.copyElement copier idx (.applyAsInt ->n idx)))
                  (.endValue out-writer)))
              out-vec)
            (catch Throwable e
              (.close out-vec)
              (throw e))))))))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface Kernel
  (^void applyAll [^core2.vector.IIndirectRelation inRelation,
                   ^core2.vector.IVectorWriter outVector])
  (^void apply [^core2.vector.IIndirectRelation inRelation,
                ^ints idxs,
                ^core2.vector.IVectorWriter outVector]))

(def ^java.util.Map call-kernels
  (HashMap.))

(defn- call-kernel-cache-key [{:keys [f arg-types]}]
  {:f f
   :arg-types (for [^FieldType arg-type arg-types]
                (.getType arg-type))
   :none-null? (every? #(not (.isNullable ^FieldType %)) arg-types)})

(defmacro with-kernel-cached [->kernel expr]
  `(let [expr# ~expr]
     (.computeIfAbsent call-kernels
                       (call-kernel-cache-key expr#)
                       (reify Function
                         (apply [_# _k#]
                           ~->kernel)))))

(defn- eval-kernel [{:keys [arg-types] :as expr}]
  (-> (let [none-null? (every? #(not (.isNullable ^FieldType %)) arg-types)
            idx-sym (gensym 'idx)
            out-vec-sym (gensym 'out-vec)
            arg-syms (for [arg-type arg-types]
                       {:arg-type arg-type
                        :col-sym (gensym 'arg-col)
                        :vec-sym (gensym 'arg-vec)
                        :arg-idx-sym (gensym 'arg-idx)})
            {:keys [return-type code]} (codegen-call (-> expr
                                                         (assoc :emitted-args (for [{:keys [^FieldType arg-type vec-sym arg-idx-sym]} arg-syms]
                                                                                (get-value-form (.getType arg-type) vec-sym arg-idx-sym))
                                                                :arg-types (for [^FieldType arg-type arg-types]
                                                                             (.getType arg-type)))))]
        {:return-type (FieldType. (not none-null?) return-type nil)

         :kernel
         (-> `(reify Kernel
                (~'applyAll [_# in-rel# out-writer#]
                 (let [~(-> out-vec-sym
                            (with-tag (types/arrow-type->vector-type return-type)))
                       (.getVector out-writer#)

                       [~@(map (comp #(with-tag % IIndirectVector) :col-sym) arg-syms)] (seq in-rel#)

                       ~@(->> (for [{:keys [^FieldType arg-type col-sym vec-sym]} arg-syms]
                                [(-> vec-sym
                                     (with-tag (types/arrow-type->vector-type (.getType arg-type))))
                                 `(.getVector ~col-sym)])
                              (apply concat))
                       row-count# (.rowCount in-rel#)]

                   (.setValueCount ~out-vec-sym row-count#)

                   (dotimes [~idx-sym row-count#]
                     (.startValue out-writer#)
                     (let [~@(->> (for [{:keys [arg-idx-sym col-sym]} arg-syms]
                                    [arg-idx-sym `(.getIndex ~col-sym ~idx-sym)])
                                  (apply concat))]
                       ~(let [set-value-code (set-value-form return-type out-vec-sym idx-sym code)]
                          (if none-null?
                            set-value-code
                            `(when (and ~@(for [{:keys [arg-idx-sym vec-sym]} arg-syms]
                                            `(not (.isNull ~vec-sym ~arg-idx-sym))))
                               ~set-value-code))))
                     (.endValue out-writer#)))))
             #_(doto clojure.pprint/pprint)
             eval)})
      #_(with-kernel-cached expr)))

(defmethod emit-expr :call [{:keys [args] :as expr} ^String col-name opts]
  (let [emitted-args (->> args
                          (into [] (map-indexed (fn [idx arg]
                                                  (emit-expr arg (str "arg" idx) opts)))))]
    (fn [^IIndirectRelation in-rel, ^BufferAllocator al, params]
      (let [evald-args (LinkedList.)]
        (try
          (doseq [eval-arg emitted-args]
            (.add evald-args (eval-arg in-rel al params)))

          (let [arg-types (mapv (fn [^ValueVector arg-vec]
                                  (.getFieldType (.getField arg-vec)))
                                evald-args)
                {:keys [^FieldType return-type ^Kernel kernel]} (eval-kernel (assoc expr :arg-types arg-types))
                ^ValueVector out-vec (-> return-type
                                         (.createNewSingleVector col-name al nil))]
            (try
              (.applyAll kernel
                         (iv/->indirect-rel (map iv/->direct-vec evald-args) (.rowCount in-rel))
                         (vw/vec->writer out-vec))
              out-vec
              (catch Throwable e
                (.close out-vec)
                (throw e))))
          (finally
            (run! util/try-close evald-args)))))))

(defn ->var-fields [^IIndirectRelation in-rel, expr]
  (->> (for [var-sym (->> (walk/expr-seq expr)
                          (into #{} (comp (filter #(= :variable (:op %)))
                                          (map :variable))))]
         (let [^IIndirectVector ivec (or (.vectorForName in-rel (name var-sym))
                                         (throw (IllegalArgumentException. (str "missing read-col: " var-sym))))]
           (MapEntry/create var-sym (-> ivec (.getVector) (.getField)))))
       (into {})))

(defn ->expression-projection-spec ^core2.operator.IProjectionSpec [^String col-name form col-names params]
  (let [expr (-> (form->expr form {:col-names col-names, :params params})
                 (macro/macroexpand-all))]
    (reify IProjectionSpec
      (getColumnName [_] col-name)

      (project [_ allocator in-rel]
        (let [eval-expr (emit-expr expr col-name {:var-fields (->var-fields in-rel expr)})]
          (iv/->direct-vec (eval-expr in-rel allocator params)))))))

(defn ->expression-relation-selector ^core2.operator.IRelationSelector [form col-names params]
  (let [projector (->expression-projection-spec "select" (list 'boolean form) col-names params)]
    (reify IRelationSelector
      (select [_ al in-rel]
        (with-open [selection (.project projector al in-rel)]
          (let [^BitVector sel-vec (.getVector selection)
                res (IntStream/builder)]
            (dotimes [idx (.getValueCount selection)]
              (when (= 1 (.get sel-vec (.getIndex selection idx)))
                (.add res idx)))
            (.toArray (.build res))))))))

(defn eval-scalar-value [al form col-names params]
  (let [expr (form->expr form {:params params})]
    (case (:op expr)
      :literal form
      :param (get params (:param expr))

      ;; this is probably quite heavyweight to calculate a single value...
      (let [projection-spec (->expression-projection-spec "_scalar" form col-names params)]
        (with-open [out-vec (.project projection-spec al (iv/->indirect-rel [] 1))]
          (types/get-object (.getVector out-vec) (.getIndex out-vec 0)))))))
