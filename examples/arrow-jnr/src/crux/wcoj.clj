(ns crux.wcoj
  (:require [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [crux.datalog :as cd])
  (:import [clojure.lang IPersistentCollection IPersistentMap Repeat]))

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

(defn- rule? [f]
  (s/valid? :crux.datalog/rule f))

(defn- can-unify? [x y]
  (or (= x y)
      (cd/prolog-var? x)
      (cd/prolog-var? y)))

(defn unify [x y]
  (when (can-unify? x y)
    (if (cd/prolog-var? x)
      [y y]
      [x x])))

(defn unified-tuple [tuple bindings]
  (let [result (mapv unify tuple bindings)]
    (when (every? identity result)
      (mapv first result))))

(defn- interleave-all
  ([])
  ([c1] c1)
  ([c1 & colls]
   (lazy-seq
    (when-let [ss (seq (remove empty? (cons c1 colls)))]
      (concat (map first ss) (apply interleave-all (map rest ss)))))) )

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
        free-vars (->> (map (comp :variable second) external-query)
                       (concat head-vars)
                       (apply disj (set (find-vars predicate))))
        body-without-not (remove (set not-predicate) body)
        body-vars (set (find-vars body-without-not))]
    (assert (= (distinct head-vars)
               (find-vars head)) "argument names cannot be reused")
    (assert (set/superset? body-vars (set head-vars))
            "rule does not satisfy safety requirement for head variables")
    (assert (set/superset? body-vars (set (find-vars not-predicate)))
            "rule does not satisfy safety requirement for not clauses")
    {:rule-name symbol
     :free-vars free-vars
     :head head
     :body (vec (concat body-without-not not-predicate))}))

(def ^:private ^:const internal-chunk-size 128)

(defn- terms->bindings [terms]
  (vec (for [[type term] terms]
         (if (= :constant type)
           (gensym 'constant)
           term))))

(defn- terms->clojure [terms]
  (vec (for [[type term] terms]
         (if (and (= :constant type)
                  (symbol? term))
           (list 'quote term)
           term))))

(defn- query-plan->clojure [{:keys [rule-name free-vars head body] :as query-plan}]
  (let [db-sym (gensym 'db)
        bindings (for [[type literal] body]
                   (case type
                     :predicate
                     (let [{:keys [symbol terms]} literal
                           arg-vars (find-vars terms)]
                       (assert (= arg-vars (distinct arg-vars)) "argument variables cannot be reused")
                       `[chunk#
                         (->> (crux.wcoj/table-filter
                               (crux.wcoj/relation-by-name ~db-sym '~symbol)
                               ~db-sym ~(terms->clojure terms))
                              (partition-all ~internal-chunk-size))
                         ~(terms->bindings terms)
                         chunk#])

                     :equality-predicate
                     (let [{:keys [lhs op rhs]} literal
                           args (terms->clojure [lhs rhs])
                           op-fn (get '{!= (complement crux.wcoj/unify)} op op)]
                       (if (= '= op)
                         `[:let [[~@(terms->bindings [lhs rhs]) :as unified?#] (crux.wcoj/unify ~@args)]
                           :when unified?#]
                         `[:when (~op-fn ~@args)]))

                     :not-predicate
                     (let [{:keys [symbol terms]} (:predicate literal)]
                       [:when `(empty? (crux.wcoj/table-filter
                                        (crux.wcoj/relation-by-name ~db-sym '~symbol)
                                        ~db-sym ~(terms->clojure terms)))])

                     :external-query
                     (let [{:keys [variable symbol terms]} literal]
                       [:let [variable `(~symbol ~@(terms->clojure terms))]])))
        args (terms->bindings (:terms head))]
    `(fn ~rule-name
       ([~db-sym] (~rule-name ~db-sym [~@(repeat (count args) ''_)]))
       ([~db-sym args#]
        (for [loop# ['~'_]
              :let [~args args#
                    [~@args :as unified?#] (crux.wcoj/unified-tuple
                                            args# ~(terms->clojure (:terms head)))
                    ~@(interleave free-vars (repeat ''_))]
              :when unified?#
              ~@(apply concat bindings)]
          ~args)))))

(def ^:private compile-rule (memoize
                             (fn [rule]
                               (eval (query-plan->clojure (rule->query-plan rule))))))

(defrecord RuleRelation [rules]
  Relation
  (table-scan [this db]
    (table-filter this db (repeat '_)))

  (table-filter [this db var-bindings]
    (let [db (vary-meta db update :rule-table-state #(or % (atom {})))
          {:keys [rule-table-state]} (meta db)
          key-var-bindings (if (instance? Repeat var-bindings)
                             nil
                             (for [v var-bindings]
                               (if (cd/prolog-var? v)
                                 '_
                                 v)))
          rule-table @rule-table-state
          memo-level (mod (count rule-table) (inc (count rules)))
          memo-key [(System/identityHashCode rules) key-var-bindings memo-level]
          memo-value (get rule-table memo-key ::not-found)]
      (if (= ::not-found memo-value)
        ((fn step [[rule & rules]]
           (lazy-seq
            (when rule
              (interleave-all
               (doto ((compile-rule rule) db var-bindings)
                 (->> (swap! rule-table-state update memo-key interleave-all)))
               (step rules))))) rules)
        memo-value)))

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

  (table-filter [this db vars])

  (insert [this tuple]
    (throw (UnsupportedOperationException.)))

  (delete [this tuple]
    (throw (UnsupportedOperationException.)))

  IPersistentCollection
  (table-scan [this db]
    (seq this))

  (table-filter [this db vars]
    (for [tuple (table-scan this db)
          :when (unified-tuple tuple vars)]
      tuple))

  (insert [this tuple]
    (conj this tuple))

  (delete [this tuple]
    (disj this tuple)))

(defrecord CombinedRelation [rules tuples]
  Relation
  (table-scan [this db]
    (->> (interleave-all (table-scan tuples db)
                         (table-scan rules db))
         (dedupe)))

  (table-filter [this db vars]
    (->> (interleave-all (table-filter tuples db vars)
                         (table-filter rules db vars))
         (dedupe)))

  (insert [this value]
    (if (rule? value)
      (update this :rules insert value)
      (update this :tuples insert value)))

  (delete [this value]
    (if (rule? value)
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

(defn tuple->datalog-str [relation-name tuple]
  (str relation-name
       (when (seq tuple)
         (str "(" (str/join ", " tuple) ")"))
       "."))

(defn execute-datalog
  ([datalog]
   (execute-datalog {} datalog))
  ([db datalog]
   (s/assert :crux.datalog/program datalog)
   (reduce
    (fn [db [type statement]]
      (case type
        :query
        (let [{{:keys [symbol]} :head} statement]
          (doseq [tuple (query-conformed-datalog db statement)]
            (println (tuple->datalog-str symbol tuple)))
          db)

        :requirement
        (do (require (first (:identifier statement)))
            db)

        (:assertion :retraction)
        (let [op (get {:assertion assertion :retraction retraction} type)
              [type clause] (:clause statement)
              {:keys [symbol terms]} (:head clause)]
          (case type
            :fact
            (op db symbol (mapv second terms))

            :rule
            (op db symbol (vec (s/unform :crux.datalog/rule clause)))))))
    db (s/conform :crux.datalog/program datalog))))
