(ns crux.z-curve
  (:require [clojure.string :as str])
  (:import java.nio.ByteBuffer
           java.util.Arrays))

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
(defn in-h-range? [^long min ^long max ^long h]
  (= (bit-and (bit-or h min) max) h))

;; inc, z has to already be in range.
(defn inc-h-in-range ^long [^long min ^long max ^long h]
  (let [;; first, fill all 'invalid' bits with '1' (bits that can have only one value).
        next-h (bit-or h (bit-not max))
        ;; increment. The '1's in the invalid bits will cause bitwise overflow to the next valid bit.
        next-h (inc next-h)
        ;; remove invalid bits.
        next-h (bit-or (bit-and next-h max) min)]
    (if (<= next-h h)
      -1
      next-h)))

;; succ, h can be anywhere.
(defn successor-h ^long [^long min ^long max ^long h]
  (let [conflict-min (Long/highestOneBit (bit-or (bit-and (bit-not h) min) 1))
        conflict-max (Long/highestOneBit (bit-or (bit-and h (bit-not max)) 1))
        mask-min (dec conflict-min)
        mask-max (dec conflict-max)
        ;; first, fill all 'invalid' bits with '1' (bits that can have only one value).
        next-h (bit-or h (bit-not max))
        ;; Set trailing bit after possible conflict to '0'
        next-h (bit-and next-h (bit-not (bit-or mask-min mask-max)))
        ;; increment. The '1's in the invalid bits will cause bitwise overflow to the next valid bit.
        ;; maskMin ensures that we don't add anything if the most significant conflict was a min-conflict
        next-h (+ next-h (bit-and conflict-max (bit-not mask-min)))]
    ;; remove invalid bits.
    (bit-or (bit-and next-h max) min)))

(def ^:const ^{:tag 'long} max-dims Byte/SIZE)

(defn dims->hyper-quads ^long [^long dims]
  (assert (<= dims max-dims))
  (max (bit-shift-left 2 (dec dims)) 1))

(defn hyper-quads->dims ^long [^long hyper-quads]
  (Long/numberOfTrailingZeros hyper-quads))

(defn decode-h-at-level ^long [^bytes z-address ^long dims ^long level]
  (assert (<= dims max-dims))
  (let [h-mask (dec (dims->hyper-quads dims))
        start-bit (* dims level)
        end-bit (dec (+ dims start-bit))
        start-byte (bit-shift-right start-bit 3)
        end-byte (bit-shift-right end-bit 3)
        shift (dec (- Byte/SIZE (bit-and end-bit (dec Byte/SIZE))))]
    (loop [idx start-byte
           z 0]
      (let [z (bit-or (bit-shift-left z Byte/SIZE)
                      (if (< idx (alength z-address))
                        (aget z-address idx)
                        (byte 0)))]
        (if (= idx end-byte)
          (bit-and (unsigned-bit-shift-right z shift) h-mask)
          (recur (inc idx) z))))))

(defn encode-h-at-level ^bytes [^bytes z-address ^long dims ^long level ^long h]
  (assert (<= dims max-dims))
  (let [h-mask (dec (dims->hyper-quads dims))
        start-bit (* dims level)
        end-bit (dec (+ dims start-bit))
        start-byte (bit-shift-right start-bit 3)
        end-byte (bit-shift-right end-bit 3)
        shift (dec (- Byte/SIZE (bit-and end-bit (dec Byte/SIZE))))]
    (aset z-address
          start-byte
          (unchecked-byte
           (bit-or (bit-and (bit-shift-right (bit-not h-mask) shift)
                            (aget z-address start-byte))
                   (bit-shift-left h shift))))
    (when-not (= start-byte end-byte)
      (aset z-address
            end-byte
            (unchecked-byte
             (bit-or (bit-and (bit-shift-left (bit-not h-mask) shift)
                              (aget z-address end-byte))
                     (bit-shift-right h shift)))))
    z-address))

(defn propagate-min-h-mask ^long [^long h ^long min ^long min-mask]
  (bit-and (bit-not (bit-xor h min)) min-mask))

(defn propagate-max-h-mask ^long [^long h ^long max ^long max-mask]
  (bit-or (bit-xor h max) max-mask))

;; TODO: This should be able to tell us the next valid z address
;; prefix using successor-h at the diffing level.
(defn in-z-range? [^bytes min-z ^bytes max-z ^bytes z ^long dims]
  (if (or (zero? dims)
          (and (Arrays/equals min-z max-z)
               (Arrays/equals max-z z)))
    true
    (let [hyper-quads (dims->hyper-quads dims)
          h-mask (dec hyper-quads)
          max-level (quot (* Byte/SIZE (min (alength min-z) (alength max-z) (alength z))) dims)]
      (loop [level 0
             min-h-mask h-mask
             max-h-mask 0]
        (let [h (decode-h-at-level z dims level)
              min-h (bit-and (decode-h-at-level min-z dims level) min-h-mask)
              max-h (bit-or (decode-h-at-level max-z dims level) max-h-mask)]
          (if (in-h-range? min-h max-h h)
            (if (or (and (zero? min-h-mask) (= h-mask max-h-mask))
                    (= max-level level))
              true
              (recur (inc level)
                     (propagate-min-h-mask h min-h min-h-mask)
                     (propagate-max-h-mask h max-h max-h-mask)))
            false))))))

;; Should double check example and algorithm 6.5.2 in Lawder on page 127.
;; http://www.dcs.bbk.ac.uk/~jkl/thesis.pdf

(def ^:private ^{:tag 'longs} dimension-masks
  (long-array (cons 0
                    (for [n (range 1 Long/SIZE)]
                      (reduce
                       (fn [^long acc ^long b]
                         (bit-or acc (bit-shift-left 1 b)))
                       0
                       (range 0 Long/SIZE n))))))

(defn z-get-next-address [^long start ^long end ^long dims]
  (let [first-differing-bit (Long/numberOfLeadingZeros (bit-xor start end))
        split-dimension (rem first-differing-bit dims)
        dimension-inherit-mask (Long/rotateLeft (aget dimension-masks dims) split-dimension)

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

(defn z-range-search [^long start ^long end ^long z ^long dims]
  (loop [start start
         end end]
    (cond
      (neg? (Long/compareUnsigned end z))
      (doto (long-array 2)
        (aset 0 end)
        (aset 1 0))

      (neg? (Long/compareUnsigned z start))
      (doto (long-array 2)
        (aset 0 0)
        (aset 1 start))

      :else
      (let [litmax+bigmin ^"[J" (z-get-next-address start end dims)
            litmax (aget litmax+bigmin 0)
            bigmin (aget litmax+bigmin 1)]
        (cond
          (neg? (Long/compareUnsigned bigmin z))
          (recur bigmin end)

          (neg? (Long/compareUnsigned z litmax))
          (recur start litmax)

          :else
          (doto (long-array 2)
            (aset 0 litmax)
            (aset 1 bigmin)))))))

(defn get-partial-long ^long [^ByteBuffer b ^long idx]
  (let [remaining (- (.capacity b) idx)]
    (if (>= remaining Long/BYTES)
      (.getLong b idx)
      (let [ba (byte-array Long/BYTES)]
        (-> b (.position idx) (.get ba 0 remaining) (.rewind))
        (.getLong (ByteBuffer/wrap ba) 0)))))

(defn put-partial-long ^java.nio.ByteBuffer [^ByteBuffer b ^long idx ^long l]
  (let [remaining (- (.capacity b) idx)]
    (if (>= remaining Long/BYTES)
      (.putLong b idx l)
      (let [ba (byte-array Long/BYTES)]
        (.putLong (ByteBuffer/wrap ba) 0 l)
        (-> b (.position idx) (.put ba 0 remaining) (.rewind))))))

(defn z-get-next-address-arrays [^bytes start ^bytes end ^long dims]
  (let [n (Arrays/mismatch start end)]
    (if (= -1 n)
      [start end]
      (let [length (alength start)
            bigmin (ByteBuffer/wrap (Arrays/copyOf start length))
            litmax (ByteBuffer/wrap (Arrays/copyOf end length))
            start-n (get-partial-long bigmin n)
            end-n (get-partial-long litmax n)]
        (let [first-differing-bit (Long/numberOfLeadingZeros (bit-xor start-n end-n))
              split-dimension (rem first-differing-bit dims)
              dimension-inherit-mask (Long/rotateLeft (aget dimension-masks dims) split-dimension)

              common-most-significant-bits-mask (bit-shift-left -1 (- Long/SIZE first-differing-bit))
              all-common-bits-mask (bit-or dimension-inherit-mask common-most-significant-bits-mask)

              ;; 1000 -> 1000000
              next-dimension-above (bit-shift-left 1 (dec (- Long/SIZE first-differing-bit)))
              _ (put-partial-long bigmin n (bit-or (bit-and all-common-bits-mask start-n) next-dimension-above))

              ;; 0111 -> 0010101
              other-dimensions-mask (bit-not dimension-inherit-mask)
              next-dimension-below (bit-and (dec next-dimension-above) other-dimensions-mask)
              _ (put-partial-long litmax n (bit-or (bit-and all-common-bits-mask end-n) next-dimension-below))]
          (loop [n (+ n Long/BYTES)]
            (if (>= n length)
              [(.array litmax)
               (.array bigmin)]
              (do (put-partial-long bigmin n (bit-or dimension-inherit-mask start-n))
                  (put-partial-long litmax n (bit-or (bit-and dimension-inherit-mask end-n)
                                                     other-dimensions-mask))
                  (recur (+ n Long/BYTES))))))))))

(defn z-range-search-arrays [^bytes start ^bytes end ^bytes z ^long dims]
  (assert (= (alength start) (alength end)))
  (loop [start start
         end end]
    (cond
      (neg? (Arrays/compareUnsigned end z))
      [end nil]

      (neg? (Arrays/compareUnsigned z start))
      [nil start]

      :else
      (let [[^bytes litmax ^bytes bigmin] (z-get-next-address-arrays start end dims)]
        (cond
          (neg? (Arrays/compareUnsigned bigmin z))
          (recur bigmin end)

          (neg? (Arrays/compareUnsigned z litmax))
          (recur start litmax)

          :else
          [litmax bigmin])))))

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
        z (byte-array (max Long/BYTES (* dims max-len)))]
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

(defn inc-unsigned-bytes ^bytes [^bytes bs]
  (loop [idx (dec (alength bs))]
    (when-not (neg? idx)
      (let [b (Byte/toUnsignedInt (aget bs idx))]
        (if (= 0xff b)
          (do (aset bs idx (byte 0))
              (recur (dec idx)))
          (doto bs
            (aset idx (unchecked-byte (inc b)))))))))

(defn dec-unsigned-bytes ^bytes [^bytes bs]
  (loop [idx (dec (alength bs))]
    (when-not (neg? idx)
      (let [b (Byte/toUnsignedInt (aget bs idx))]
        (if (zero? b)
          (do (aset bs idx (byte 0xff))
              (recur (dec idx)))
          (doto bs
            (aset idx (unchecked-byte (dec b)))))))))
