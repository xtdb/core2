(ns crux.wcoj
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [crux.datalog :as cd])
  (:import [clojure.lang IPersistentCollection IPersistentMap
            LineNumberingPushbackReader Symbol]
           java.io.StringReader))

(set! *unchecked-math* :warn-on-boxed)
(s/check-asserts true)

;; generic-join

(defprotocol Relation
  (table-scan [this db])
  (table-filter [this db var-bindings])
  (insert [this value])
  (delete [this value]))

(defprotocol Db
  (assertion [this relation-name value])
  (retraction [this relation-name value])
  (ensure-relation [this relation-name relation-factory])
  (relation-by-name [this relation-name]))

(defprotocol Unification
  (unify [this that]))

(extend-protocol Unification
  Symbol
  (unify [this that]
    (if (cd/logic-var? that)
      [this this]
      (if (or (= this that)
              (cd/logic-var? this))
        [that that])))

  IPersistentCollection
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (coll? that)
        (mapv unify this that))))

  Object
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (= this that)
        [this this]))))

(defn- find-vars [body]
  (let [vars (atom [])]
    (w/postwalk (fn [x]
                  (when (and (vector? x)
                             (= :variable (first x)))
                    (swap! vars conj (second x)))
                  x)
                body)
    @vars))

(defn- rule->query-plan [rule]
  (let [{:keys [head body]} (s/conform :crux.datalog/rule rule)
        {:keys [symbol terms]} head
        head-vars (find-vars head)
        {:keys [predicate not-predicate external-query]} (group-by first body)
        free-vars (apply disj (set (find-vars predicate)) head-vars)
        body-without-not (remove (set not-predicate) body)
        body-vars (set (find-vars body-without-not))]
    (assert (= (distinct head-vars)
               (find-vars head)) "argument names cannot be reused")
    (assert (set/superset? body-vars (set head-vars))
            "rule does not satisfy safety requirement for head variables")
    (assert (set/superset? body-vars (set (find-vars not-predicate)))
            "rule does not satisfy safety requirement for not clauses")
    (doseq [literal predicate
            :let [{:keys [symbol terms]} literal
                  arg-vars (find-vars terms)]]
      (assert (= arg-vars (distinct arg-vars))
              "predicate argument variables cannot be reused"))
    {:free-vars free-vars
     :rule rule
     :head head
     :body (vec (concat body-without-not not-predicate))}))

(defn- terms->bindings [terms]
  (vec (for [[type term] terms]
         (if (= :constant type)
           (gensym 'constant)
           term))))

(defn- terms->values [terms]
  (vec (for [[type term] terms]
         (if (and (= :constant type)
                  (symbol? term))
           (list 'quote term)
           term))))

(defn- predicate->clojure [{:keys [db-sym]} {:keys [symbol terms]}]
  `(crux.wcoj/table-filter
    (crux.wcoj/relation-by-name ~db-sym '~symbol)
    ~db-sym ~(terms->values terms)))

(defn- unification->clojure [bindings x y]
  `[:let [[~@bindings :as unified?#] (crux.wcoj/unify ~x ~y)]
    :when unified?#])

(defmulti ^:private datalog->clojure (fn [query-plan [type]]
                                       type))

(defmethod datalog->clojure :predicate [query-plan [_ {:keys [terms] :as predicate}]]
  [(terms->bindings terms) (predicate->clojure query-plan predicate)])

(defmethod datalog->clojure :equality-predicate [_ [_ {:keys [lhs op rhs]}]]
  (let [bindings (terms->bindings [lhs rhs])
        [lhs-arg rhs-arg :as args] (terms->values [lhs rhs])
        op-fn (get '{!= (complement crux.wcoj/unify)} op op)]
    (if (= '= op)
      (unification->clojure bindings lhs-arg rhs-arg)
      `[:when (~op-fn ~@args)])))

(defmethod datalog->clojure :not-predicate [query-plan [_ {:keys [predicate]}]]
  `[:when (empty? ~(predicate->clojure query-plan predicate))])

(defmethod datalog->clojure :external-query [_ [_ {:keys [variable symbol terms]}]]
  (unification->clojure [variable] variable `(~symbol ~@(terms->values terms))))

(defn- query-plan->clojure [{:keys [free-vars head body] :as query-plan}]
  (let [{:keys [symbol terms]} head
        db-sym (gensym 'db)
        args-sym (gensym 'args)
        query-plan (assoc query-plan :db-sym db-sym)
        bindings (mapcat (partial datalog->clojure query-plan) body)
        args (terms->bindings terms)]
    `(fn ~symbol
       ([~db-sym] (~symbol ~db-sym [~@(repeat (count args) ''_)]))
       ([~db-sym ~args-sym]
        (for [loop# ['~'_]
              :let [~@(interleave free-vars (repeat ''_))
                    ~args ~args-sym]
              ~@(unification->clojure (map vector args)
                 args-sym (terms->values terms))
              ~@bindings]
          ~args)))))

(defn- compile-rule-no-memo [rule]
  (let [query-plan (rule->query-plan rule)
        clojure-source (query-plan->clojure query-plan)]
    (with-meta
      (eval clojure-source)
      (assoc query-plan :source clojure-source))))

(def ^:private compile-rule (memoize compile-rule-no-memo))

(defn- normalize-vars [vars]
  (for [v vars]
    (if (cd/logic-var? v)
      '_
      v)))

(defn- execute-rules-no-memo [rules db var-bindings]
  (->> (for [rule rules]
         (if (empty? var-bindings)
           ((compile-rule rule) db)
           ((compile-rule rule) db var-bindings)))
       (apply concat)
       (distinct)))

(defn- execute-rules [rules db var-bindings]
  (let [db (vary-meta db update :rule-memo-state #(or % (atom {})))
        {:keys [rule-memo-state]} (meta db)
        memo-key [(System/identityHashCode rules) (normalize-vars var-bindings)]
        memo-value (get @rule-memo-state memo-key ::not-found)]
    (if (= ::not-found memo-value)
      (doto (execute-rules-no-memo rules db var-bindings)
        (->> (swap! rule-memo-state assoc memo-key)))
      memo-value)))

(defrecord RuleRelation [rules]
  Relation
  (table-scan [this db]
    (table-filter this db nil))

  (table-filter [this db var-bindings]
    (execute-rules rules db var-bindings))

  (insert [this rule]
    (s/assert :crux.datalog/rule rule)
    (update this :rules conj rule))

  (delete [this rule]
    (update this :rules disj rule)))

(defn- new-rule-relation []
  (->RuleRelation #{}))

(extend-protocol Relation
  nil
  (table-scan [this db])

  (table-filter [this db var-bindings])

  (insert [this tuple]
    (throw (UnsupportedOperationException.)))

  (delete [this tuple]
    (throw (UnsupportedOperationException.)))

  IPersistentCollection
  (table-scan [this db]
    (seq this))

  (table-filter [this db var-bindings]
    (for [tuple (table-scan this db)
          :when (unify-tuple tuple var-bindings)]
      tuple))

  (insert [this tuple]
    (conj this tuple))

  (delete [this tuple]
    (disj this tuple)))

(defrecord CombinedRelation [rules tuples]
  Relation
  (table-scan [this db]
    (concat (table-scan tuples db)
            (table-scan rules db)))

  (table-filter [this db var-bindings]
    (concat (table-filter tuples db var-bindings)
            (table-filter rules db var-bindings)))

  (insert [this value]
    (if (s/valid? :crux.datalog/rule value)
      (update this :rules insert value)
      (update this :tuples insert value)))

  (delete [this value]
    (if (s/valid? :crux.datalog/rule value)
      (update this :rules delete value)
      (update this :tuples delete value))))

(defn- new-combined-relation []
  (->CombinedRelation (new-rule-relation) (sorted-set)))

(extend-type IPersistentMap
  Db
  (assertion [this relation-name value]
    (update (ensure-relation this relation-name new-combined-relation)
            relation-name
            insert
            value))

  (retraction [this relation-name value]
    (update this
            relation-name
            delete
            value))

  (ensure-relation [this relation-name relation-factory]
    (cond-> this
      (not (contains? this relation-name)) (assoc relation-name (relation-factory))))

  (relation-by-name [this relation-name]
    (get this relation-name)))

(defn query-by-name
  ([db rule-name]
   (table-scan (relation-by-name db rule-name) db))
  ([db rule-name args]
   (table-filter (relation-by-name db rule-name) db args)))

(defn- query-conformed-datalog [db {{:keys [symbol terms]} :head}]
  (let [args (mapv second terms)]
    (query-by-name db symbol args)))

(defn query-datalog [db query]
  (s/assert :crux.datalog/query query)
  (query-conformed-datalog db (s/conform :crux.datalog/query query)))

(defn assert-all [db relation-name tuples]
  (reduce (fn [db tuple]
            (assertion db relation-name tuple)) db tuples))

(defn tuple->datalog-str [relation-name tuple]
  (str relation-name
       (when (seq tuple)
         (str "(" (str/join ", " tuple) ")"))
       "."))

(def ^:private execution-hierarchy
  (-> (make-hierarchy)
      (derive :assertion :modification)
      (derive :retraction :modification)
      (atom)))

(defmulti ^:private execute-statement
  (fn [db [type]]
    type)
  :hierarchy execution-hierarchy)

(defmethod execute-statement :query [db [type {{:keys [symbol]} :head :as statement}]]
  (doseq [tuple (dedupe (query-conformed-datalog db statement))]
    (println (tuple->datalog-str symbol tuple)))
  db)

(defmethod execute-statement :requirement [db [type {:keys [identifier]}]]
  (require (first identifier))
  db)

(defmethod execute-statement :modification [db [type {:keys [clause]}]]
  (let [op (get {:assertion assertion :retraction retraction} type)
        [type clause] clause
        {:keys [symbol terms]} (:head clause)]
    (case type
      :fact
      (op db symbol (mapv second terms))

      :rule
      (op db symbol (vec (s/unform :crux.datalog/rule clause))))))

(defn execute-datalog
  ([datalog]
   (execute-datalog {} datalog))
  ([db datalog]
   (s/assert :crux.datalog/program datalog)
   (->> (s/conform :crux.datalog/program datalog)
        (reduce execute-statement db))))

(defn parse-datalog [datalog-source]
  (let [in (LineNumberingPushbackReader.
            (if (string? datalog-source)
              (StringReader. ^String datalog-source)
              (io/reader datalog-source)))]
    (->> (repeatedly #(edn/read {:eof nil} in))
         (take-while identity)
         (s/assert :crux.datalog/program))))

(defn -main [& [f :as args]]
  (execute-datalog (parse-datalog (io/reader (or f *in*)))))
