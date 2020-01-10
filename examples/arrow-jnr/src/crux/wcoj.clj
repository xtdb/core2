(ns crux.wcoj
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.walk :as w]
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

(defrecord RuleRelation [rule-fns]
  Relation
  (table-scan [this db]
    (table-filter this db (repeat '_)))

  (table-filter [this db var-bindings]
    (->> (for [rule rule-fns
               :let [memo-key [rule (if (instance? Repeat var-bindings)
                                      nil
                                      var-bindings)]]
               :when (not (contains? (:rule-memo (meta db)) memo-key))]
           (rule (vary-meta db update :rule-memo (fnil conj #{}) memo-key) var-bindings))
         (apply concat)))

  (insert [this rule]
    (assert (and (fn? rule) (::clojure-source (meta rule)))
            "a rule needs to be a function")
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
    (update (ensure-relation this relation-name (if (fn? value)
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

(defn tuple->datalog-str [symbol tuple]
  (str symbol "(" (str/join ", " tuple) ")."))

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
        :query (let [{:keys [literal]} body
                     [type body] literal]
                 (case type
                   :predicate
                   (let [{:keys [symbol terms]} body]
                     (doseq [tuple (query-datalog db symbol (repeat (count terms) '_))]
                       (println (tuple->datalog-str symbol terms)))
                     db)))
        :retraction (let [{:keys [clause]} body
                          [type body] clause]
                      (case type
                        :fact (let [[type body] body]
                                (case type
                                  :predicate
                                  (let [{:keys [symbol terms]} body]
                                    (retraction db symbol (mapv second terms)))))))
        :assertion (let [{:keys [clause]} body
                         [type body] clause]
                     (case type
                       :fact (let [[type body] body]
                               (case type
                                 :predicate
                                 (let [{:keys [symbol terms]} body]
                                   (assertion db symbol (mapv second terms)))))
                       :rule (let [{:keys [literal body]} body
                                   [literal-type literal-body] literal]
                               (case literal-type
                                 :predicate
                                 (let [{:keys [symbol terms]} literal-body
                                       args (mapv second terms)
                                       _ (assert (= args (distinct args))
                                                 "argument names cannot be reused")
                                       free-vars (apply disj (find-vars body) args)
                                       fn (list 'fn symbol
                                                (list '[db] (cons symbol (cons 'db (vec (repeat (count terms) (list 'quote '_))))))
                                                (list ['db args]
                                                      (list 'let
                                                            [(vec free-vars)
                                                             (vec (repeat (count free-vars) (list 'quote '_)))]
                                                            (cons 'for
                                                                  [(->> (for [[literal-type literal] body]
                                                                          (case literal-type
                                                                            :predicate (let [{:keys [symbol terms]} literal
                                                                                             vars (mapv second terms)]
                                                                                         [vars (list 'crux.wcoj/table-filter (list 'crux.wcoj/relation-by-name 'db (list 'quote symbol)) 'db vars)])
                                                                            :equality-predicate (let [{:keys [lhs op rhs]} literal]
                                                                                                  (list :when (list (get '{!= not=} op op) (second lhs) (second rhs))))))
                                                                        (reduce into ['_ [(list 'quote '_)]]))
                                                                   (mapv second terms)]))))]
                                   (assertion db symbol (with-meta (eval fn) {::datalog-head literal-body
                                                                              ::datalog-body body
                                                                              ::clojure-source fn})))))))
        db))
    db (s/conform :crux.datalog/program datalog))))

(comment
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
        db {}
        db (compile-datalog db triangle)
        result (query-datalog db 'q)]
    (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]} (set result)))

  (let [edge '[edge(1, 2).
               edge(2, 3).
               path(X, Y) :- edge(X, Y).
               path(X, Z) :- path(X, Y), edge(Y, Z).]
        db {}
        db (compile-datalog db edge)
        result (query-datalog db 'path)]
    (= #{[1 2] [2 3] [1 3]} (set result))))

;; Simplistic spike using binary strings for z-order to
;; explore the algorithms described in the papers:

;; Optimal Joins using Compact Data Structures
;; https://arxiv.org/abs/1908.01812
;; Worst-Case Optimal Radix Triejoin
;; https://arxiv.org/abs/1912.12747
;; https://brodyf.github.io/thesis.pdf

(def ^:dynamic ^{:tag 'long} *binary-str-length* Long/SIZE)

(defn ->binary-str ^String [^long i]
  (str/replace
   (format (str "%" *binary-str-length* "s") (Long/toUnsignedString i 2))
   \space \0))

(defn parse-binary-str ^Number [^String b]
  (if (> (count b) Long/SIZE)
    (BigInteger. b 2)
    (Long/parseUnsignedLong b 2)))

(defn dimension ^long [^String b]
  (quot (count b) *binary-str-length*))

(defn interleave-bits ^String [bs]
  (->> (for [b bs]
         (cond-> b
           (int? b) (->binary-str)))
       (apply interleave)
       (str/join)))

(defn as-path
  ([^String b]
   (as-path b (dimension b)))
  ([^String b ^long d]
   (map str/join (partition d b))))

(defn select-in-path [p idxs]
  (for [p p]
    (str/join (map (vec p) idxs))))

(defn component [^String b ^long idx]
  (str/join (take-nth (dimension b) (drop idx b))))

(defn components [^String b idxs]
  (for [i idxs]
    (component b i)))

;; An example database for this query on domain {0, 1, 2} is R(A, B) =
;; {(0, 1)}, S(B,C) = {(1, 2)}, T(A, C) = {(0, 2)} and a possible
;; Booleanisation of the relations could be R(2) (A0, A1, B0, B1) =
;; {(0, 0, 0, 1)}, S(2) (B0, B1 , C0, C1) = {(0, 1, 1, 0)}, T(2)(A0,
;; A1, C0, C1) = {(0, 0, 1, 0)}

(comment
  ;; bits
  (let [r (interleave-bits [0 1])
        s (interleave-bits [1 2])
        t (interleave-bits [0 2])
        expected-q (interleave-bits [0 1 2])

        find-vars '[A B C]
        joins [['A [[r 0]
                    [t 0]]]
               ['B [[r 1]
                    [s 0]]]
               ['C [[s 1]
                    [t 1]]]]

        result-tuple (for [[_ join] joins
                           :let [vs (for [[rel col] join]
                                      (component rel col))]
                           :while (apply = vs)]
                       (first vs))
        result (when (= (count find-vars)
                        (count result-tuple))
                 (interleave-bits result-tuple))]
    [expected-q
     result
     (= expected-q result)
     (->> (range (count find-vars))
          (components result)
          (mapv parse-binary-str))
     find-vars
     joins
     (s/conform :crux.datalog/program
                '[r(0, 1).
                  s(1, 2).
                  t(0, 2).

                  q(A, B, C) :- r(A, B), s(B, C), t(A, C).])]))
