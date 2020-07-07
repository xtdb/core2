(ns crux.timeline)

(set! *unchecked-math* :warn-on-boxed)

;; Potentially useful for incremental index maintenance.

(defn quick-select
  (^long [^longs a ^long x]
   (quick-select a 0 (dec (alength a)) x))
  (^long [^longs a ^long low ^long hi ^long x]
   (loop [i low
          j low]
     (if (<= j hi)
       (let [tmp (aget a j)]
         (if (< tmp x)
           (do (doto a
                 (aset j (aget a i))
                 (aset i tmp))
               (recur (inc i) (inc j)))
           (recur i (inc j))))
       (let [tmp (aget a hi)]
         (doto a
           (aset hi (aget a i))
           (aset i tmp))
         i)))))

(defn quick-sort
  ([^longs a]
   (quick-sort a 0 (dec (alength a))))
  ([^longs a ^long low ^long hi]
   (when (< low hi)
     (let [p (quick-select a low hi (aget a hi))]
       (quick-sort a low (dec p))
       (quick-sort a (inc p) hi)))
   a))
