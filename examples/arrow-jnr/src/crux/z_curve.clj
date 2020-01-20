(ns crux.z-curve
  (:require [clojure.string :as str]))

(set! *unchecked-math* :warn-on-boxed)

;; Simplistic spike using binary strings for z-order to
;; explore the algorithms described in the papers:

;; Optimal Joins using Compact Data Structures
;; https://arxiv.org/abs/1908.01812
;; Worst-Case Optimal Radix Triejoin
;; https://arxiv.org/abs/1912.12747
;; https://brodyf.github.io/thesis.pdf

;; Relation to BDDs?
;; https://github.com/petablox/petablox/wiki/Datalog-Analysis
;; https://people.csail.mit.edu/mcarbin/papers/aplas05.pdf

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
     joins]))

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

;; 00 11 00 ;; 12
;; 00 01 00 ;;  4 working min mask

;; 10 11 01 ;; 45
;; 10 11 11 ;; 47 working max mask

;; 01 00 11 ;; 19

;; 10 01 00 ;; 36

;; min/max masks:

;; For the lower limit, a '1' indicates that the 'lower' half of this
;; dimension does NOT need to be queried.

;; For the upper limit, a '1' indicates that the 'higher' half DOES
;; need to be queried.

;; isInI
;; min | pos == pos ;; no bits in min cannot be in pos (also max)
;; max & pos == pos ;; all bits in pos has to be in max
;; min & ~max == 0  ;; all bits in min has to be in max

;; min mask: any bit that's always set in range
;; max mask: any bit that's ever set in range
(defn in-z-range? [^long min ^long max ^long z]
  (= (bit-and (bit-or z min) max) z))

;; inc, z has to already be in range.
(defn inc-z-in-range ^long [^long min ^long max ^long z]
  (let [;; first, fill all 'invalid' bits with '1' (bits that can have only one value).
        next-z (bit-or z (bit-not max))
        ;; increment. The '1's in the invalid bits will cause bitwise overflow to the next valid bit.
        next-z (inc next-z)
        ;; remove invalid bits.
        next-z (bit-or (bit-and next-z max) min)]
    (if (<= next-z z)
      -1
      next-z)))

;; succ, z can be anywhere.
(defn successor-z ^long [^long min ^long max ^long z]
  (let [conflict-min (Long/highestOneBit (bit-or (bit-and (bit-not z) min) 1))
        conflict-max (Long/highestOneBit (bit-or (bit-and z (bit-not max)) 1))
        mask-min (dec conflict-min)
        mask-max (dec conflict-max)
        ;; first, fill all 'invalid' bits with '1' (bits that can have only one value).
        next-z (bit-or z (bit-not max))
        ;; Set trailing bit after possible conflict to '0'
        next-z (bit-and next-z (bit-not (bit-or mask-min mask-max)))
        ;; increment. The '1's in the invalid bits will cause bitwise overflow to the next valid bit.
        ;; maskMin ensures that we don't add anything if the most significant conflict was a min-conflict
        next-z (+ next-z (bit-and conflict-max (bit-not mask-min)))]
    ;; remove invalid bits.
    (bit-or (bit-and next-z max) min)))

;; Should double check example and algorithm 6.5.2 in Lawder on page 127.
;; http://www.dcs.bbk.ac.uk/~jkl/thesis.pdf
