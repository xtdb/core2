(ns crux.timeline
  (:import java.util.Comparator))

(set! *unchecked-math* :warn-on-boxed)

;; Potentially useful for incremental index maintenance.

(defn upper-int ^long [^long x]
  (unsigned-bit-shift-right x Integer/SIZE))

(defn lower-int ^long [^long x]
  (bit-and x (Integer/toUnsignedLong -1)))

(defn two-ints-as-long ^long [^long x ^long y]
  (bit-or (bit-shift-left x Integer/SIZE) y))

(defn long-to-two-ints ^longs [^long x]
  (doto (long-array 2)
    (aset 0 (upper-int x))
    (aset 1 (lower-int x))))

(defn three-way-partition
  (^long [^longs a ^long pivot]
   (three-way-partition a 0 (dec (alength a)) pivot))
  (^long [^longs a ^long low ^long hi ^long pivot]
   (loop [i low
          j low
          k (inc hi)]
     (if (< j k)
       (let [aj (aget a j)
             diff (- aj pivot)]
         (cond
           (neg? diff)
           (do (doto a
                 (aset j (aget a i))
                 (aset i aj))
               (recur (inc i) (inc j) k))

           (pos? diff)
           (let [k (dec k)]
             (doto a
               (aset j (aget a k))
               (aset k aj))
             (recur i j k))

           :else
           (recur i (inc j) k)))
       (two-ints-as-long i (dec k))))))

(defn quick-sort
  (^longs [^longs a]
   (quick-sort a 0 (dec (alength a))))
  (^longs [^longs a ^long low ^long hi]
   (if (< low hi)
     (let [pivot (aget a hi)
           left-right (three-way-partition a low hi pivot)
           left (dec (upper-int left-right))
           right (inc (lower-int left-right))]
       (if (< (- hi right) (- left low))
         (do (quick-sort a right hi)
             (recur a low left))
         (do (quick-sort a low left)
             (recur a right hi))))
     a)))

(defn crack-column [{:index/keys [^longs column pieces] :as index} ^long at]
  (if (contains? pieces at)
    index
    (->> (if (empty? pieces)
           (three-way-partition column at)
           (if-let [next-pieces (not-empty (subseq pieces >= at))]
             (let [[[_ ^long next-piece-pos]] next-pieces
                   [[_ prev-piece-pos]] (rsubseq pieces < at)]
               (three-way-partition column (or prev-piece-pos 0) (dec next-piece-pos) at))
             (let [[_ last-piece-pos] (last pieces)]
               (three-way-partition column last-piece-pos (dec (alength column)) at))))
         (upper-int)
         (assoc-in index [:index/pieces at]))))

(defn ->index [^longs ls]
  {:index/column ls :index/pieces (sorted-map)})

(comment
  (-> (->index (long-array [13 16 4 9 2 12 7 1 19 3 14 11 8 6]))
      (crack-column 11)
      (crack-column 14)
      (crack-column 8)
      (crack-column 17))

  #:index{:column [6, 4, 3, 2, 7, 1, 8, 9, 11, 13, 12, 14, 16, 19],
          :pieces {8                 6,
                   11                      8,
                   14                                  11,
                   17                                          13}})

;; https://stratos.seas.harvard.edu/files/IKM_CIDR07.pdf

;; Algorithm 1 CrackInTwo(c,posL,posH,med,inc)
;; Physically reorganize the piece of column c between posL
;; and posH such that all values lower than med are in a contiguous space. inc indicates whether med is inclusive or not,
;; e.g., if inc = f alse then θ1 is “<” and θ2 is “>=”
;; 1: x1 = point at position posL
;; 2: x2 = point at position posH
;; 3: while position(x1) < position(x2) do
;; 4: if value(x1) θ1 med then
;; 5: x1 = point at next position
;; 6: else
;; 7: while value(x2) θ2 med &&
;; position(x2) > position(x1) do
;; 8: x2 = point at previous position
;; 9: end while
;; 10: exchange(x1,x2)
;; 11: x1 = point at next position
;; 12: x2 = point at previous position
;; 13: end if
;; 14: end while
