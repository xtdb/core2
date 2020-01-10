(ns crux.wcoj
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [crux.datalog])
  (:import clojure.lang.IPersistentVector))

;; Simplistic spike using binary strings for z-order to
;; explore the algorithms described in the papers:

;; Optimal Joins using Compact Data Structures
;; https://arxiv.org/abs/1908.01812
;; Worst-Case Optimal Radix Triejoin
;; https://arxiv.org/abs/1912.12747
;; https://brodyf.github.io/thesis.pdf

(set! *unchecked-math* :warn-on-boxed)

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
                '[r(0, 2).
                  s(1, 2).
                  t(0, 2).

                  q(A, B, C) :- r(A, B), s(B, C), t(A, C).])]))

;; generic-join

(defn can-unify-var? [value bound-var]
  (or (= '_ bound-var)
      (= value bound-var)))

(defn can-unify-tuple? [tuple bindings]
  (->> (map can-unify-var? tuple bindings)
       (every? true?)))

(defprotocol Relation
  (table-scan [this])
  (table-filter [this vars]))

(extend-type IPersistentVector
  Relation
  (table-scan [this]
    (seq this))

  (table-filter [this vars]
    (for [tuple (table-scan this)
          :when (can-unify-tuple? tuple vars)]
      tuple)))

(comment

  (let [db {'r [[1 3]
                [1 4]
                [1 5]
                [3 5]]
            't [[1 4]
                [1 5]
                [1 6]
                [1 8]
                [1 9]
                [1 2]
                [3 2]]
            's [[3 4]
                [3 5]
                [4 6]
                [4 8]
                [4 9]
                [5 2]]

            'q (fn q
                 ([db]
                  (q db '_ '_ '_))
                 ([db a b c]
                  (for [[a b] (table-filter (get db 'r) [a b])
                        [b c] (table-filter (get db 's) [b c])
                        [a c] (table-filter (get db 't) [a c])]
                    [a b c])))}

        result ((get db 'q) db)]
    (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]} (set result))))
