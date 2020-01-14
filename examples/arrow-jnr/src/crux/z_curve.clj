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
;; https://github.com/tzaeschke/phtree/blob/master/src/main/java/ch/ethz/globis/phtree/v16/NodeIteratorNoGC.java#L106

;; succ
;; https://github.com/tzaeschke/phtree/blob/master/src/test/java/ch/ethz/globis/phtree/bits/TestIncSuccessor.java#L227
;; inc
;; https://github.com/tzaeschke/phtree/blob/master/src/test/java/ch/ethz/globis/phtree/bits/TestIncSuccessor.java#L425
;; checkHcPos
;; https://github.com/tzaeschke/phtree/blob/master/src/test/java/ch/ethz/globis/phtree/bits/TestIncSuccessor.java#L440

;; https://github.com/tzaeschke/phtree/blob/master/src/test/java/ch/ethz/globis/phtree/bits/TestIncrementor.java

;; query entry point:
;; https://github.com/tzaeschke/phtree/blob/master/src/main/java/ch/ethz/globis/phtree/v16/PhIteratorNoGC.java#L97

;; ([4 47 "010100" "101111" (12 13 14 15 36 37 38 39 44 45)])

;; (defn is-in-i? [^long h ^long m0 ^long m1]
;;   (= (bit-and (bit-or h m0) m1) h))

;; (defn inc-h ^long [^long h ^long m0 ^long m1]
;;   (if (< h m1)
;;     (let [h-out (bit-or h (bit-not m1))
;;           h-out (inc h-out)]
;;       (bit-or (bit-and h-out m1) m0))
;;     -1))

;; ;; (crux.z-curve/z-seq 12 4 47)
;; ;; 12 -> 4  ;; m1 - bit is set
;; ;; 45 -> 47 ;; m2 - bit is not set
;; (defn z-seq [^long h ^long m0 ^long m1]
;;   (when (<= h m1)
;;     (->> (iterate (fn ^long [^long h]
;;                     (inc-h h m0 m1)) h)
;;          (take-while nat-int?)
;;          #_(filter #(is-in-i? % m0 m1))
;;          (drop-while (fn [^long x] (< x h)))
;;          (take-while (fn [^long x] (<= x m1))))))

;; previous version of this spike:

;; isInI
(defn in-z-range? [^long start ^long end ^long z]
  (= (bit-and (bit-or z start) end) z))

;; inc, z has to already be in range.
(defn next-in-range [^long start ^long end ^long z]
  (let [next-z (bit-or (bit-and (inc (bit-or z (bit-not end))) end) start)]
    (if (<= next-z z)
      -1
      next-z)))

;; succ, z can be anywhere.
(defn next-within-range [^long start ^long end ^long z]
  (let [mask-start (dec (Long/highestOneBit (bit-or (bit-and (bit-not z) start) 1)))
        end-high-bit (Long/highestOneBit (bit-or (bit-and z (bit-not end)) 1))
        mask-end (dec end-high-bit)
        next-z (bit-or z (bit-not end))
        next-z (bit-and next-z (bit-not (bit-or mask-start mask-end)))
        next-z (+ next-z (bit-and end-high-bit (bit-not mask-start)))]
    (bit-or (bit-and next-z end) start)))
