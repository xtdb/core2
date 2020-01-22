(ns crux.wcoj
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [crux.datalog :as cd])
  (:import [clojure.lang IPersistentCollection IPersistentMap Symbol]))

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
      (when (and (coll? that)
                 (= (count this) (count that)))
        (first
         (reduce
          (fn [[acc smap] [x y]]
            (or (when-let [[xu yu] (unify (get smap x x) (get smap y y))]
                  [(conj acc [xu yu])
                   (assoc smap x xu y yu)])
                (reduced nil)))
          [[] {}]
          (mapv vector this that))))))

  Object
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (= this that)
        [this this])))

  nil
  (unify [this that]
    (when (nil? that)
      [nil nil])))

(defn- find-vars [body]
  (let [vars (atom [])]
    (w/postwalk (fn [x]
                  (when (cd/logic-var? x)
                    (swap! vars conj x))
                  x)
                body)
    (set @vars)))

(defn- ensure-unique-logic-var [var]
  (if (cd/logic-var? var)
    (gensym var)
    var))

(defn- ensure-unique-anonymous-var [var]
  (if (= '_ var)
    (gensym '_)
    var))

(defn- contains-duplicate-vars? [var-bindings]
  (let [vars (filter cd/logic-var? var-bindings)]
    (not= (distinct vars) vars)))

(def ^:private literal-hierarchy
  (-> (make-hierarchy)
      (derive :equality-predicate :predicate)
      (atom)))

(defmulti ^:private new-bound-vars
  (fn [known-vars extra-logical-vars [type]]
    type)
  :hierarchy literal-hierarchy)

(defmethod new-bound-vars :predicate [known-vars extra-logical-vars [_ literal]]
  (let [vars (find-vars literal)]
    (when (set/superset? known-vars (set/intersection vars extra-logical-vars))
      vars)))

(defmethod new-bound-vars :not-predicate [known-vars _ [_ literal]]
  (let [vars (find-vars literal)]
    (when (set/superset? known-vars vars)
      #{})))

(defmethod new-bound-vars :external-query [known-vars _ [_ {:keys [terms variable]}]]
  (when (set/superset? known-vars (find-vars terms))
    #{variable}))

(defn- reorder-body [head body]
  (let [{:keys [external-query] :as literals} (group-by first body)
        extra-logical-vars (set (for [[_ {:keys [variable]}] external-query]
                                  variable))]
    (loop [[[type :as literal] & new-body :as body] body
           acc []
           known-vars (set/difference (find-vars head) extra-logical-vars)
           circular-check 0]
      (if literal
        (if-let  [new-vars (new-bound-vars known-vars extra-logical-vars literal)]
          (recur new-body (conj acc literal) (set/union known-vars new-vars) 0)
          (if (= (count body) circular-check)
            (throw (IllegalArgumentException. "Circular dependency."))
            (recur (conj (vec new-body) literal) acc known-vars (inc circular-check))))
        acc))))

(defn- rule->query-plan [rule]
  (let [rule (w/postwalk ensure-unique-anonymous-var rule)
        {:keys [head body]} (s/conform :crux.datalog/rule rule)
        {:keys [symbol terms]} head
        head-vars (find-vars head)
        body (reorder-body head body)
        body-vars (find-vars body)]
    (assert (set/superset? body-vars head-vars)
            "rule does not satisfy safety requirement for head variables")
    {:existential-vars (set/difference body-vars head-vars)
     :rule rule
     :head head
     :body body}))

(defn- term->binding [[type term]]
  (if (= :constant type)
    (gensym 'constant)
    term))

(defn- quote-term [x]
  (list 'quote x))

(defn- term->value [[type term]]
  (if (and (= :constant type)
           (symbol? term))
    (quote-term term)
    term))

(defn- predicate->clojure [{:keys [db-sym]} {:keys [symbol terms]}]
  `(crux.wcoj/table-filter
    (crux.wcoj/relation-by-name ~db-sym '~symbol)
    ~db-sym ~(mapv term->value terms)))

(defn- unification->clojure [bindings x y]
  `[:let [[~@bindings :as unified?#] (crux.wcoj/unify ~x ~y)]
    :when unified?#])

(defmulti ^:private datalog->clojure (fn [query-plan [type]]
                                       type))

(defmethod datalog->clojure :predicate [query-plan [_ {:keys [terms] :as predicate}]]
  (let [term-vars (mapv term->binding terms)]
    [term-vars (predicate->clojure query-plan predicate)]))

(defmethod datalog->clojure :equality-predicate [_ [_ {:keys [lhs op rhs]}]]
  (let [op-fn (get '{!= (complement crux.wcoj/unify)} op op)]
    (if (= '= op)
      (unification->clojure
       [(term->binding lhs) (term->binding rhs)]
       (term->value lhs) (term->value rhs))
      `[:when (~op-fn ~(term->value lhs) ~(term->value rhs))])))

(defmethod datalog->clojure :not-predicate [query-plan [_ {:keys [predicate]}]]
  `[:when (empty? ~(predicate->clojure query-plan predicate))])

(defmethod datalog->clojure :external-query [_ [_ {:keys [variable symbol terms]}]]
  (unification->clojure [variable] variable `(~symbol ~@(mapv term->value terms))))

(defn- query-plan->clojure [{:keys [existential-vars head body] :as query-plan}]
  (let [{:keys [symbol terms]} head
        db-sym (gensym 'db)
        args-sym (gensym 'args)
        query-plan (assoc query-plan :db-sym db-sym)
        bindings (mapcat (partial datalog->clojure query-plan) body)
        arg-vars (mapv term->binding terms)
        args-signature (quote-term (mapv second terms))]
    `(fn ~symbol
       ([~db-sym] (~symbol ~db-sym '~(vec (repeatedly (count arg-vars) #(ensure-unique-anonymous-var '_)))))
       ([~db-sym ~args-sym]
        (for [loop# [nil]
              ~@(unification->clojure (map vector arg-vars) args-sym args-signature)
              :let [~@(interleave existential-vars (map quote-term existential-vars))]
              ~@bindings]
          ~arg-vars)))))

(defn- compile-rule-no-memo [rule]
  (let [query-plan (rule->query-plan rule)
        clojure-source (query-plan->clojure query-plan)]
    (with-meta
      (eval clojure-source)
      (assoc query-plan :source clojure-source))))

(def ^:private compile-rule (memoize compile-rule-no-memo))

(defn- execute-rules-no-memo [rules db var-bindings]
  (->> (for [rule rules]
         (if (empty? var-bindings)
           ((compile-rule rule) db)
           ((compile-rule rule) db var-bindings)))
       (apply concat)
       (distinct)))

(defn- rule-memo-key [rules var-bindings]
  [(System/identityHashCode rules)
   (-> (zipmap (filter cd/logic-var? var-bindings)
               (for [id (range)]
                 (symbol (str "crux.wcoj/variable_" id))))
       (replace var-bindings))])

(defn- execute-rules-memo [rules db var-bindings]
  (let [db (vary-meta db update :rule-memo-state #(or % (atom {})))
        {:keys [rule-memo-state]} (meta db)
        memo-key (rule-memo-key rules var-bindings)
        memo-value (get @rule-memo-state memo-key ::not-found)]
    (if (= ::not-found memo-value)
      (doto (execute-rules-no-memo rules db var-bindings)
        (->> (swap! rule-memo-state assoc memo-key)))
      memo-value)))

(defn- execute-rules [rules db var-bindings]
  (cond->> (execute-rules-memo rules db (mapv ensure-unique-logic-var var-bindings))
    (contains-duplicate-vars? var-bindings) (filter #(unify var-bindings %))))

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
          :when (unify tuple var-bindings)]
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

(defn query [db query]
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
  (doseq [tuple (sort (distinct (query-conformed-datalog db statement)))]
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

(defn execute
  ([datalog]
   (execute {} datalog))
  ([db datalog]
   (s/assert :crux.datalog/program datalog)
   (->> (s/conform :crux.datalog/program datalog)
        (reduce execute-statement db))))

(defn -main [& [f :as args]]
  (execute-datalog (cd/parse-datalog (io/reader (or f *in*)))))
