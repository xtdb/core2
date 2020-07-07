(ns crux.timeline)

(set! *unchecked-math* :warn-on-boxed)

;; Potentially useful for incremental index maintenance.

(defn- scan-forward ^long [^longs a ^long i ^long j ^long x]
  (loop [i (inc i)]
    (if (and (<= i j)
             (< (aget a i) x))
      (recur (inc i))
      i)))

(defn- scan-backward ^long [^longs a ^long i ^long j ^long x]
  (loop [j (dec j)]
    (if (and (>= j i)
             (> (aget a j) x))
      (recur (dec j))
      j)))

(defn quick-select
  (^long [^longs a ^long x]
   (quick-select a 0 (dec (alength a)) x))
  (^long [^longs a ^long low ^long high ^long x]
   (loop [i (dec low)
          j (inc high)]
     (let [i (scan-forward a i j x)
           j (scan-backward a i j x)]
       (if (>= i j)
         j
         (let [tmp (aget a j)]
           (doto a
             (aset j (aget a i))
             (aset i tmp))
           (recur i j)))))))

(defn quick-sort
  ([^longs a]
   (quick-sort a 0 (dec (alength a))))
  ([^longs a ^long low ^long hi]
   (when (< low hi)
     (let [pivot (aget a (quot (+ hi low) 2))
           p (quick-select a low hi pivot)]
       (quick-sort a low p)
       (quick-sort a (inc p) hi)))
   a))
