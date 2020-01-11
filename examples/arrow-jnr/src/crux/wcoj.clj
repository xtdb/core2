(ns crux.wcoj
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.test :as t]
            [crux.datalog])
  (:import [clojure.lang IPersistentCollection IPersistentMap Repeat]))

(set! *unchecked-math* :warn-on-boxed)
(s/check-asserts true)

;; generic-join

(defn- can-unify-var? [value bound-var]
  (or (= '_ bound-var)
      (= value bound-var)))

(defn- can-unify-tuple? [tuple bindings]
  (->> (map can-unify-var? tuple bindings)
       (every? true?)))

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

(defn- rule-fn? [f]
  (s/valid? :crux.datalog/rule f))

(defn- interleave-all [colls]
  (lazy-seq
   (when-let [ss (seq (remove empty? colls))]
     (concat (map first ss) (interleave-all (map rest ss))))) )

(def ^:private ^:const internal-chunk-size 128)

(defn- find-vars [body]
  (let [vars (atom #{})]
    (w/postwalk (fn [x]
                  (when (and (vector? x)
                             (= :variable (first x)))
                    (swap! vars conj (second x)))
                  x)
                body)
    @vars))

(defn- rule->clojure [rule-source]
  (let [{:keys [literal body]} (s/conform :crux.datalog/rule rule-source)
        [type literal] literal]
    (case type
      :predicate
      (let [{:keys [symbol terms]} literal
            args (mapv second terms)
            _ (assert (= args (distinct args)) "argument names cannot be reused")
            {:keys [predicate arithmetic external-query]} (group-by first body)
            free-vars (->> (map (comp :variable second) (concat arithmetic external-query))
                           (concat args)
                           (apply disj (find-vars predicate)))
            db-sym (gensym 'db)]
        `(fn ~symbol
           ([~db-sym] (~symbol ~db-sym [~@(repeat (count terms) ''_)]))
           ([~db-sym ~args]
            (let [~@(interleave free-vars (repeat ''_))]
              (for ~(->> (for [[type literal] body]
                           (case type
                             :predicate
                             (let [{:keys [symbol terms]} literal
                                   chunk-sym (gensym 'chunk)
                                   chunk-size internal-chunk-size]
                               [chunk-sym
                                `(->> (crux.wcoj/table-filter
                                       (crux.wcoj/relation-by-name ~db-sym '~symbol)
                                       ~db-sym ~(mapv second terms))
                                      (partition-all ~chunk-size))
                                (vec (for [[type term] terms]
                                       (if (= :constant type)
                                         (gensym 'constant)
                                         term)))
                                chunk-sym])

                             :not
                             (let [{:keys [symbol terms]} (:predicate literal)]
                               [:when `(empty? (crux.wcoj/table-filter
                                                (crux.wcoj/relation-by-name ~db-sym '~symbol)
                                                ~db-sym ~(mapv second terms)))])

                             :external-query
                             (let [{:keys [variable symbol terms]} literal]
                               [:let [variable `(~symbol ~@(mapv second terms))]])

                             :arithmetic
                             (let [{:keys [variable lhs op rhs]} literal
                                   op (get '{% mod} op op)]
                               [:let [variable `(~op ~@(map second (remove nil? [lhs rhs])))]])

                             :equality-predicate
                             (let [{:keys [lhs op rhs]} literal
                                   op (get '{!= not=} op op)]
                               [:when `(~op ~@(map second [lhs rhs]))])))
                         (reduce into [(gensym 'loop) [''_]]))
                ~args))))))))

(def ^:private compile-rule (memoize
                             (fn [rule]
                               (eval (rule->clojure rule)))))

(defrecord RuleRelation [rule-fns]
  Relation
  (table-scan [this db]
    (table-filter this db (repeat '_)))

  (table-filter [this db var-bindings]
    (let [db (vary-meta db update :rule-table #(or % (atom {})))
          {:keys [rule-recursion-guard rule-table]} (meta db)
          key-var-bindings (if (instance? Repeat var-bindings)
                             nil
                             var-bindings)
          guard-key [(System/identityHashCode rule-fns) key-var-bindings]
          db (vary-meta db update :rule-recursion-guard (fnil conj #{}) guard-key)]
      (when-not (contains? rule-recursion-guard guard-key)
        (->> (for [rule rule-fns
                   :let [memo-key [(System/identityHashCode rule) key-var-bindings]]]
               (if (contains? @rule-table memo-key)
                 (get @rule-table memo-key)
                 (doto ((compile-rule rule) db var-bindings)
                   (->> (swap! rule-table assoc memo-key)))))
             (interleave-all)))))

  (insert [this rule]
    (assert (rule-fn? rule) "not a rule")
    (update this :rule-fns conj rule))

  (delete [this rule]
    (update this :rule-fns disj rule)))

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
    (interleave-all [(table-scan tuples db)
                     (table-scan rules db)]))

  (table-filter [this db vars]
    (interleave-all [(table-filter tuples db vars)
                     (table-filter rules db vars)]))

  (insert [this value]
    (if (rule-fn? value)
      (update this :rules insert value)
      (update this :tuples insert value)))

  (delete [this value]
    (if (rule-fn? value)
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

(defn- normalize-name [s]
  (symbol (name s)))

(defn query-datalog
  ([db rule-name]
   (table-scan (relation-by-name db (normalize-name rule-name)) db))
  ([db rule-name args]
   (table-filter (relation-by-name db (normalize-name rule-name)) db args)))

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
        (let [[type literal] (:literal statement)]
          (case type
            :predicate
            (let [{:keys [symbol terms]} literal
                  args (for [[type term] terms]
                         (if (= :variable type)
                           '_
                           term))]
              (doseq [tuple (query-datalog db symbol args)]
                (println (tuple->datalog-str symbol tuple)))
              db)))

        (:assertion :retraction)
        (let [op (get {:assertion assertion :retraction retraction} type)
              [type clause] (:clause statement)]
          (case type
            :fact
            (let [[type literal] (:literal clause)]
              (case type
                :predicate
                (let [{:keys [symbol terms]} literal]
                  (op db symbol (mapv second terms)))))

            :rule
            (let [[type literal] (:literal clause)]
              (case type
                :predicate
                (let [{:keys [symbol]} literal]
                  (op db symbol (vec (s/unform :crux.datalog/rule clause))))))))
        db))
    db (s/conform :crux.datalog/program datalog))))

(t/deftest triangle-join-query
  (let [triangle '[r(1, 3).
                   r(1, 4).
                   r(1, 5).
                   r(3, 5).

                   t(1, 2).
                   t(1, 4).
                   t(1, 5).
                   t(1, 6).
                   t(1, 8).
                   t(1, 9).
                   t(3, 2).

                   s(3, 4).
                   s(3, 5).
                   s(4, 6).
                   s(4, 8).
                   s(4, 9).
                   s(5, 2).

                   q(A, B, C) :- r(A, B), s(B, C), t(A, C).]
        db (execute-datalog triangle)
        result (query-datalog db 'q)]
    (t/is (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]}
             (set result)))))

(t/deftest edge-recursion-rules
  (let [edge '[edge(1, 2).
               edge(2, 3).

               path(X, Y) :- edge(X, Y).
               path(X, Z) :- path(X, Y), edge(Y, Z).]
        db (execute-datalog edge)
        result (query-datalog db 'path)]
    (t/is (= #{[1 2] [2 3] [1 3]} (set result)))))

(t/deftest fib-using-interop
  (let [fib '[fib(0, 0).
              fib(1, 1).

              fib(N, F) :-
              N > 1,
              N1 is N - 1,
              N2 is N - 2,
              fib(N1, F1),
              fib(N2, F2),
              F is F1 + F2 .]
        db (execute-datalog fib)
        result (query-datalog db 'fib '[30 F])]
    (t/is (= #{[30 832040]} (set result)))))

;; https://www.swi-prolog.org/pldoc/man?section=tabling-non-termination
(t/deftest connection-recursion-rules
  (let [connection '[connection(X, Y) :-
                     connection(X, Z),
                     connection(Z, Y).

                     connection(X, Y) :-
                     connection(Y, X).

                     connection("Amsterdam", "Schiphol").
                     connection("Amsterdam", "Haarlem").
                     connection("Schiphol", "Leiden").
                     connection("Haarlem", "Leiden").]
        db (execute-datalog connection)
        result (query-datalog db 'connection '["Amsterdam" _])]
    (t/is (= #{["Amsterdam" "Haarlem"]
               ["Amsterdam" "Schiphol"]
               ["Amsterdam" "Amsterdam"]
               ["Amsterdam" "Leiden"]} (set result)))))
