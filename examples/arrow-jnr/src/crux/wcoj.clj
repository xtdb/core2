(ns crux.wcoj
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.walk :as w]
            [crux.datalog])
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

(defn- can-unify-var? [value bound-var]
  (or (= '_ bound-var)
      (= value bound-var)))

(defn- can-unify-tuple? [tuple bindings]
  (->> (map can-unify-var? tuple bindings)
       (every? true?)))

(defn- interleave-all
  ([])
  ([c1] c1)
  ([c1 & colls]
   (lazy-seq
    (when-let [ss (seq (remove empty? (cons c1 colls)))]
      (concat (map first ss) (apply interleave-all (map rest ss)))))) )

(defn- find-vars [body]
  (let [vars (atom #{})]
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
        args (mapv second terms)
        _ (assert (= args (distinct args)) "argument names cannot be reused")
        {:keys [predicate not-predicate external-query]} (group-by first body)
        free-vars (->> (map (comp :variable second) external-query)
                       (concat args)
                       (apply disj (find-vars predicate)))
        body (concat (remove (set not-predicate) body) not-predicate)]
    {:rule-name symbol
     :args args
     :free-vars free-vars
     :body body}))

(def ^:private ^:const internal-chunk-size 128)

(defn- query-plan->clojure [{:keys [rule-name args free-vars body] :as query-plan}]
  (let [db-sym (gensym 'db)
        bindings (for [[type literal] body]
                   (case type
                     :predicate
                     (let [{:keys [symbol terms]} literal]
                       `[chunk#
                         (->> (crux.wcoj/table-filter
                               (crux.wcoj/relation-by-name ~db-sym '~symbol)
                               ~db-sym ~(mapv second terms))
                              (partition-all ~internal-chunk-size))
                         ~(vec (for [[type term] terms]
                                 (if (= :constant type)
                                   (gensym 'constant)
                                   term)))
                         chunk#])

                     :equality-predicate
                     (let [{:keys [lhs op rhs]} literal
                           op (get '{!= not=} op op)]
                       [:when `(~op ~@(map second [lhs rhs]))])

                     :not-predicate
                     (let [{:keys [symbol terms]} (:predicate literal)]
                       [:when `(empty? (crux.wcoj/table-filter
                                        (crux.wcoj/relation-by-name ~db-sym '~symbol)
                                        ~db-sym ~(mapv second terms)))])

                     :external-query
                     (let [{:keys [variable symbol terms]} literal]
                       [:let [variable `(~symbol ~@(mapv second terms))]])))]
    `(fn ~rule-name
       ([~db-sym] (~rule-name ~db-sym [~@(repeat (count args) ''_)]))
       ([~db-sym ~args]
        (for [loop# ['_]
              :let [~@(interleave free-vars (repeat ''_))]
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
    (let [db (vary-meta db update :rule-table #(or % (atom {})))
          {:keys [rule-recursion-guard rule-table]} (meta db)
          key-var-bindings (if (instance? Repeat var-bindings)
                             nil
                             var-bindings)
          guard-key [(System/identityHashCode rules) key-var-bindings]
          db (vary-meta db update :rule-recursion-guard (fnil conj #{}) guard-key)]
      (when-not (contains? rule-recursion-guard guard-key)
        (->> (for [rule rules
                   :let [memo-key [(System/identityHashCode rule) key-var-bindings]
                         memo-value (get @rule-table memo-key ::not-found)]]
               (if (= ::not-found memo-value)
                 (doto ((compile-rule rule) db var-bindings)
                   (->> (swap! rule-table assoc memo-key)))
                 memo-value))
             (apply interleave-all)))))

  (insert [this rule]
    (s/assert :crux.datalog/rule rule)
    (update this :rules conj rule))

  (delete [this rule]
    (update this :rules disj rule)))

(defn- new-rule-relation []
  (->RuleRelation #{}))

(extend-type IPersistentCollection
  Relation
  (table-scan [this db]
    (seq this))

  (table-filter [this db vars]
    (for [tuple (table-scan this db)
          :when (can-unify-tuple? tuple vars)]
      tuple))

  (insert [this tuple]
    (conj this tuple))

  (delete [this tuple]
    (disj this tuple)))

(defrecord CombinedRelation [rules tuples]
  Relation
  (table-scan [this db]
    (interleave-all (table-scan tuples db)
                    (table-scan rules db)))

  (table-filter [this db vars]
    (interleave-all (table-filter tuples db vars)
                    (table-filter rules db vars)))

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

(defn query-datalog
  ([db rule-name]
   (table-scan (relation-by-name db rule-name) db))
  ([db rule-name args]
   (table-filter (relation-by-name db rule-name) db args)))

(defn tuple->datalog-str [relation-name tuple]
  (str relation-name "(" (str/join ", " tuple) ")."))

(defn execute-datalog
  ([datalog]
   (execute-datalog {} datalog))
  ([db datalog]
   (s/assert :crux.datalog/program datalog)
   (reduce
    (fn [db [type statement]]
      (case type
        :query
        (let [{:keys [symbol terms]} (:head statement)
              args (for [[type term] terms]
                     (if (= :variable type)
                       '_
                       term))]
          (doseq [tuple (query-datalog db symbol args)]
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
