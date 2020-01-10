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
  (boolean (and (fn? f) (::clojure-source (meta f)))))

(defrecord RuleRelation [rule-fns]
  Relation
  (table-scan [this db]
    (table-filter this db (repeat '_)))

  (table-filter [this db var-bindings]
    (->> (for [rule rule-fns
               :let [memo-key [(System/identityHashCode rule)
                               (if (instance? Repeat var-bindings)
                                 nil
                                 var-bindings)]]
               :when (not (contains? (:rule-memo (meta db)) memo-key))]
           (rule (vary-meta db update :rule-memo (fnil conj #{}) memo-key) var-bindings))
         (apply concat)))

  (insert [this rule]
    (assert (rule-fn? rule) "a rule needs to be a function")
    (update this :rule-fns conj rule))

  (delete [this rule]
    (throw (UnsupportedOperationException.))))

(defn- new-rule-relation []
  (->RuleRelation []))

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

(extend-type IPersistentMap
  Db
  (assertion [this relation-name value]
    (update (ensure-relation this relation-name (if (rule-fn? value)
                                                  new-rule-relation
                                                  sorted-set))
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
  ([db q]
   (table-scan (relation-by-name db q) db))
  ([db q args]
   (table-filter (relation-by-name db q) db args)))

(defn tuple->datalog-str [relation-name tuple]
  (str relation-name "(" (str/join ", " tuple) ")."))

(defn- find-vars [body]
  (let [vars (atom #{})]
    (w/postwalk (fn [x]
                  (when (and (vector? x)
                             (= :variable (first x)))
                    (swap! vars conj (second x)))
                  x)
                body)
    @vars))

(defn compile-datalog
  ([datalog]
   (compile-datalog {} datalog))
  ([db datalog]
   (s/assert :crux.datalog/program datalog)
   (reduce
    (fn [db [type body :as form]]
      (case type
        :query
        (let [{:keys [literal]} body
              [type body] literal]
          (case type
            :predicate
            (let [{:keys [symbol terms]} body
                  args (for [[type arg] terms]
                         (if (= :variable type)
                           '_
                           arg))]
              (doseq [tuple (query-datalog db symbol args)]
                (println (tuple->datalog-str symbol tuple)))
              db)))

        :retraction
        (let [{:keys [clause]} body
              [type body] clause]
          (case type
            :fact (let [[type body] body]
                    (case type
                      :predicate
                      (let [{:keys [symbol terms]} body]
                        (retraction db symbol (mapv second terms)))))))

        :assertion
        (let [{:keys [clause]} body
              [type body] clause]
          (case type
            :fact
            (let [[type body] body]
              (case type
                :predicate
                (let [{:keys [symbol terms]} body]
                  (assertion db symbol (mapv second terms)))))

            :rule
            (let [{:keys [literal body]} body
                  [literal-type literal-body] literal]
              (case literal-type
                :predicate
                (let [{:keys [symbol terms]} literal-body
                      args (mapv second terms)
                      _ (assert (= args (distinct args)) "argument names cannot be reused")
                      {:keys [predicate arithmetic external-query]} (group-by first body)
                      free-vars (->> (map (comp :variable second) (concat arithmetic external-query))
                                     (concat args)
                                     (apply disj (find-vars predicate)))
                      db-sym (gensym 'db)
                      fn-source `(fn ~symbol
                                   ([~db-sym] (~symbol ~db-sym [~@(repeat (count terms) ''_)]))
                                   ([~db-sym ~args]
                                    (let [~@(interleave free-vars (repeat ''_))]
                                      (for ~(->> (for [[literal-type literal] body]
                                                   (case literal-type
                                                     :predicate
                                                     (let [{:keys [symbol terms]} literal]
                                                       [(vec (for [[type arg] terms]
                                                               (if (= :constant type)
                                                                 (gensym 'constant)
                                                                 arg)))
                                                        `(crux.wcoj/table-filter
                                                          (crux.wcoj/relation-by-name ~db-sym '~symbol)
                                                          ~db-sym ~(mapv second terms))])

                                                     :external-query
                                                     (let [{:keys [variable external-symbol terms]} literal]
                                                       [:let [variable `(~external-symbol ~@(mapv second terms))]])

                                                     :arithmetic
                                                     (let [{:keys [variable lhs op rhs]} literal
                                                           op (get '{% mod} op op)]
                                                       [:let [variable `(~op ~@(map second (remove nil? [lhs rhs])))]])

                                                     :equality-predicate
                                                     (let [{:keys [lhs op rhs]} literal
                                                           op (get '{!= not=} op op)]
                                                       [:when `(~op ~@(map second [lhs rhs]))])))
                                                 (reduce into [(gensym 'loop) [''_]]))
                                        ~args))))
                      rule-fn (with-meta (eval fn-source) {::datalog-head literal-body
                                                           ::datalog-body body
                                                           ::clojure-source fn-source})]
                  (assertion db symbol rule-fn))))))
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
        db (compile-datalog {} triangle)
        result (query-datalog db 'q)]
    (t/is (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]}
             (set result)))))

(t/deftest edge-recursion-rules
  (let [edge '[edge(1, 2).
               edge(2, 3).

               path(X, Y) :- edge(X, Y).
               path(X, Z) :- path(X, Y), edge(Y, Z).]
        db (compile-datalog {} edge)
        result (query-datalog db 'path)]
    (t/is (= #{[1 2] [2 3] [1 3]} (set result)))))

(t/deftest fib-using-interop
  (let [fib '[fib_base(0, 0).
              fib_base(1, 1).

              fib(N, F) :- fib_base(N, F).
              fib(N, F) :-
              N > 1,
              N1 is N - 1,
              N2 is N - 2,
              fib(N1, F1),
              fib(N2, F2),
              F is F1 + F2 .]
        db (compile-datalog {} fib)
        result (query-datalog db 'fib '[15 F])]
    (t/is (= #{[15 610]} (set result)))))

;; https://www.swi-prolog.org/pldoc/man?section=tabling-non-termination
(t/deftest connection-recursion-rules
  (let [connection '[connection(X, Y) :-
                     connection(X, Z),
                     connection(Z, Y).

                     connection(X, Y) :-
                     connection_base(Y, X).

                     connection(X, Y) :-
                     connection_base(X, Y).

                     connection_base("Amsterdam", "Schiphol").
                     connection_base("Amsterdam", "Haarlem").
                     connection_base("Schiphol", "Leiden").
                     connection_base("Haarlem", "Leiden").]
        db (compile-datalog {} connection)
        result (query-datalog db 'connection '["Amsterdam" _])]
    (t/is (= #{["Amsterdam" "Haarlem"]
               ["Amsterdam" "Schiphol"]
               ["Amsterdam" "Amsterdam"]
               ["Amsterdam" "Leiden"]} (set result)))))
