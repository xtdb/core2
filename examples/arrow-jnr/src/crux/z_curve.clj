(ns crux.z-curve
  (:require [clojure.string :as str])
  (:import java.util.Arrays))

(set! *unchecked-math* :warn-on-boxed)

;; Optimal Joins using Compact Data Structures
;; https://arxiv.org/abs/1908.01812
;; Worst-Case Optimal Radix Triejoin
;; https://arxiv.org/abs/1912.12747
;; https://brodyf.github.io/thesis.pdf

;; Relation to BDDs?
;; https://github.com/petablox/petablox/wiki/Datalog-Analysis
;; https://people.csail.mit.edu/mcarbin/papers/aplas05.pdf

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

(def ^:private dimension-masks
  (long-array (cons 0
                    (for [n (range 1 Long/SIZE)]
                      (reduce
                       (fn [^long acc ^long b]
                         (bit-or acc (bit-shift-left 1 b)))
                       0
                       (range 0 Long/SIZE n))))))

(defn ^"[J" morton-get-next-address [^long start ^long end ^long dim]
  (let [first-differing-bit (Long/numberOfLeadingZeros (bit-xor start end))
        split-dimension (rem first-differing-bit dim)
        dimension-inherit-mask (Long/rotateLeft (aget ^longs dimension-masks dim) split-dimension)

        common-most-significant-bits-mask (bit-shift-left -1 (- Long/SIZE first-differing-bit))
        all-common-bits-mask (bit-or dimension-inherit-mask common-most-significant-bits-mask)

        ;; 1000 -> 1000000
        next-dimension-above (bit-shift-left 1 (dec (- Long/SIZE first-differing-bit)))
        bigmin (bit-or (bit-and all-common-bits-mask start) next-dimension-above)

        ;; 0111 -> 0010101
        next-dimension-below (bit-and (dec next-dimension-above)
                                      (bit-not dimension-inherit-mask))
        litmax (bit-or (bit-and all-common-bits-mask end) next-dimension-below)]
    (doto (long-array 2)
      (aset 0 litmax)
      (aset 1 bigmin))))

(defn morton-get-next-address-arrays [^"[J" start ^"[J" end ^long dim]
  (let [n (Arrays/mismatch start end)]
    (if (= -1 n)
      [start end]
      (let [length (alength start)
            bigmin (Arrays/copyOf start length)
            litmax (Arrays/copyOf end length)
            start-n (aget start n)
            end-n (aget end n)]
        (let [first-differing-bit (Long/numberOfLeadingZeros (bit-xor start-n end-n))
              split-dimension (rem first-differing-bit dim)
              dimension-inherit-mask (Long/rotateLeft (aget ^longs dimension-masks dim) split-dimension)

              common-most-significant-bits-mask (bit-shift-left -1 (- Long/SIZE first-differing-bit))
              all-common-bits-mask (bit-or dimension-inherit-mask common-most-significant-bits-mask)

              ;; 1000 -> 1000000
              next-dimension-above (bit-shift-left 1 (dec (- Long/SIZE first-differing-bit)))
              _ (doto bigmin
                  (aset n (bit-or (bit-and all-common-bits-mask start-n) next-dimension-above)))

              ;; 0111 -> 0010101
              other-dimensions-mask (bit-not dimension-inherit-mask)
              next-dimension-below (bit-and (dec next-dimension-above) other-dimensions-mask)
              _ (doto litmax
                  (aset n (bit-or (bit-and all-common-bits-mask end-n) next-dimension-below)))]
          (loop [n (inc n)]
            (if (= n length)
              [litmax bigmin]
              (do (doto bigmin
                    (aset n (bit-or dimension-inherit-mask (aget start n))))
                  (doto litmax
                    (aset n (bit-or (bit-and dimension-inherit-mask (aget end n))
                                    other-dimensions-mask)))
                  (recur (inc n))))))))))

;; Interleave alternatives:
;; LUT: https://github.com/kevinhartman/morton-nd
;; Magic bits: https://github.com/LLNL/rubik/blob/master/rubik/zorder.py

(defn bit-interleave ^bytes [bs]
  (let [dims (count bs)
        max-len (long (loop [acc 0
                             idx 0]
                        (if (= dims idx)
                          acc
                          (recur (max acc (alength ^bytes (nth bs idx)))
                                 (inc idx)))))
        z (byte-array (* dims max-len))]
    (dotimes [d dims]
      (let [dim-bytes ^bytes (nth bs d)]
        (dotimes [byte-idx (min max-len (alength dim-bytes))]
          (let [b (aget dim-bytes byte-idx)]
            (when-not (zero? b)
              (let [byte-bit-idx (bit-shift-left byte-idx 3)]
                (dotimes [bit-idx Byte/SIZE]
                  (when-not (zero? (bit-and b (bit-shift-left 1 (- (dec Byte/SIZE) bit-idx))))
                    (let [n (+ (* (+ byte-bit-idx bit-idx) dims) d)
                          z-byte-idx (bit-shift-right n 3)
                          z-bit-idx (bit-and n (dec Byte/SIZE))
                          z-bit (bit-shift-left 1 (- (dec Byte/SIZE) z-bit-idx))]
                      (aset z z-byte-idx (byte (bit-or (aget z z-byte-idx) z-bit))))))))))))
    z))
