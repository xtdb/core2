(ns crux.z-curve
  (:require [clojure.string :as str]
            [crux.datalog]))

(set! *unchecked-math* :warn-on-boxed)

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

;; http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F8-11-13/paper_web.pdf
;; https://github.com/tzaeschke/phtree

;; calculate m0 and m1 from range min, max (calcLimits):
;; src/main/java/ch/ethz/globis/phtree/v16hd/NodeIteratorNoGC.java

;; calculate succ (succSS, inc, checkHcPos):
;; src/test/java/ch/ethz/globis/phtree/bits/TestIncSuccessor.java
;; calculate inc:
;; src/test/java/ch/ethz/globis/phtree/bits/TestIncrementor.java

;; ([4 47 "010100" "101111" (12 13 14 15 36 37 38 39 44 45)])

(defn is-in-i? [^long h ^long m0 ^long m1]
  (= (bit-and (bit-or h m0) m1) h))

(defn inc-h ^long [^long h ^long m0 ^long m1]
  (if (< h m1)
    (let [h-out (bit-or h (bit-not m1))
          h-out (inc h-out)]
      (bit-or (bit-and h-out m1) m0))
    -1))

;; (crux.z-curve/z-seq 12 4 47)
;; 12 -> 4  ;; m1 - bit is set
;; 45 -> 47 ;; m2 - bit is not set
(defn z-seq [^long h ^long m0 ^long m1]
  (when (<= h m1)
    (->> (iterate (fn ^long [^long h]
                    (inc-h h m0 m1)) h)
         (take-while nat-int?)
         #_(filter #(is-in-i? % m0 m1))
         (drop-while (fn [^long x] (< x h)))
         (take-while (fn [^long x] (<= x m1))))))

(defn z-seq-min-max [^long h ^long min ^long max]
  (z-seq h
         (bit-shift-right (Integer/highestOneBit min) 1)
         (bit-and (dec (bit-shift-left (Integer/highestOneBit max) 1))
                  (bit-not (bit-shift-right (Integer/highestOneBit max) 1)))))
