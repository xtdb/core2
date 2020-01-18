(ns crux.wcoj
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [crux.datalog :as cd])
  (:import [clojure.lang IPersistentCollection IPersistentMap
            LineNumberingPushbackReader Repeat]
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
   (when-let [ss (seq (remove empty? (cons c1 colls)))]
     (distinct
      (concat (map first ss)
              (apply concat (map rest ss)))))) )

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
    (doseq [literal predicate
            :let [{:keys [symbol terms]} literal
                  arg-vars (find-vars terms)]]
      (assert (= arg-vars (distinct arg-vars))
              "predicate argument variables cannot be reused"))
    {:rule-name symbol
     :free-vars free-vars
     :head head
     :body (vec (concat body-without-not not-predicate))}))

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

(defmulti datalog->clojure (fn [[type] query-plan]
                             type))

(defmethod datalog->clojure :predicate [[_ {:keys [symbol terms]}] {:keys [db-sym] :as query-plan}]
  `[~(terms->bindings terms)
    (->> (crux.wcoj/table-filter
          (crux.wcoj/relation-by-name ~db-sym '~symbol)
          ~db-sym ~(terms->clojure terms)))])

(defmethod datalog->clojure :equality-predicate [[_ {:keys [lhs op rhs]}] query-plan]
  (let [args (terms->clojure [lhs rhs])
        op-fn (get '{!= (complement crux.wcoj/unify)} op op)]
    (if (= '= op)
      `[:let [[~@(terms->bindings [lhs rhs]) :as unified?#] (crux.wcoj/unify ~@args)]
        :when unified?#]
      `[:when (~op-fn ~@args)])))

(defmethod datalog->clojure :not-predicate [[_ {{:keys [symbol terms]} :predicate}] {:keys [db-sym] :as query-plan}]
  `[:when (empty? (crux.wcoj/table-filter
                   (crux.wcoj/relation-by-name ~db-sym '~symbol)
                   ~db-sym ~(terms->clojure terms)))])

(defmethod datalog->clojure :external-query [[_ {:keys [variable symbol terms]}] query-plan]
  `[:let [~variable (~symbol ~@(terms->clojure terms))]])

(defn- query-plan->clojure [{:keys [rule-name free-vars head body] :as query-plan}]
  (let [db-sym (gensym 'db)
        query-plan (assoc query-plan :db-sym db-sym)
        bindings (for [literal body]
                   (datalog->clojure literal query-plan))
        args (terms->bindings (:terms head))]
    `(fn ~rule-name
       ([~db-sym] (~rule-name ~db-sym [~@(repeat (count args) ''_)]))
       ([~db-sym args#]
        (for [loop# ['~'_]
              :let [~@(interleave free-vars (repeat ''_))
                    ~args args#
                    [~@args :as unified?#]
                    (crux.wcoj/unified-tuple
                     args# ~(terms->clojure (:terms head)))]
              :when unified?#
              ~@(apply concat bindings)]
          ~args)))))

(def ^:private compile-rule
  (memoize
   (fn [rule]
     (eval (query-plan->clojure (rule->query-plan rule))))))

(defn- normalize-vars [vars]
  (for [v vars]
    (if (cd/prolog-var? v)
      '_
      v)))

(defrecord RuleRelation [rules]
  Relation
  (table-scan [this db]
    (table-filter this db (repeat '_)))

  (table-filter [this db var-bindings]
    (let [db (vary-meta db update :rule-memo-state #(or % (atom {})))
          db (vary-meta db update :rule-depth (fnil inc 0))
          {:keys [rule-memo-state rule-depth]} (meta db)
          key-var-bindings (if (instance? Repeat var-bindings)
                             nil
                             (normalize-vars var-bindings))
          rule-memo @rule-memo-state]
      (->> (for [rule rules]
             (if (<= (long rule-depth) (count rules))
               ((compile-rule rule) db var-bindings)
               (let [memo-key [(System/identityHashCode rule) key-var-bindings]
                     memo-value (get rule-memo memo-key ::not-found)]
                 (if (= ::not-found memo-value)
                   (doto ((compile-rule rule) db var-bindings)
                     (->> (swap! rule-memo-state assoc memo-key)))
                   memo-value))))
           (apply interleave-all))))

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
    (interleave-all (table-scan tuples db)
                    (table-scan rules db)))

  (table-filter [this db vars]
    (interleave-all (table-filter tuples db vars)
                    (table-filter rules db vars)))

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
          (doseq [tuple (dedupe (query-conformed-datalog db statement))]
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
