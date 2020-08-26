(ns crux.wcoj
  (:import [java.util Arrays ArrayList NavigableSet Set TreeMap TreeSet]
           [java.util.function Function Predicate]))

(defn long-mod ^long [^long num ^long div]
  (let [m (rem num div)]
    (if (or (zero? m) (= (pos? num) (pos? div)))
      m
      (+ m div))))

(defn tree-map-put-in ^java.util.TreeMap [^TreeMap m [k & ks] v]
  (if ks
    (doto m
      (-> (.computeIfAbsent k
                            (reify Function
                              (apply [_ k]
                                (TreeMap. (.comparator m)))))
          (tree-map-put-in ks v)))
    (doto m
      (.put k v))))

(defn tuples->tree-map ^java.util.NavigableMap [tuples]
  (reduce
   (fn [acc v]
     (tree-map-put-in acc v nil))
   (TreeMap.)
   tuples))

(defn intersect-sets [xs]
  (let [key+sets ^objects (to-array
                           (sort-by first
                                    (for [x xs]
                                      (object-array [(first x) x]))))
        acc (ArrayList.)
        len (alength key+sets)]
    (loop [n 0
           max-k (first (aget key+sets (long-mod (dec n) len)))]
      (let [key+set ^objects (aget key+sets n)
            k (aget key+set 0)
            ^NavigableSet s (aget key+set 1)]
        (if-let [max-k (if (= k max-k)
                         (do (.add acc k)
                             (.higher s k))
                         (.ceiling s max-k))]
          (do (aset key+set 0 max-k)
              (recur (long-mod (inc n) len) max-k))
          acc)))))

(defn intersect-sets-arrays [xs]
  (let [key+sets ^objects (to-array
                           (sort-by first
                                    (for [x xs]
                                      (object-array [0 x]))))
        len (alength key+sets)]
    ((fn self [^long n max-k]
       (when (some? max-k)
         (let [key+set ^objects (aget key+sets n)
               kn (long (aget key+set 0))
               s ^longs (aget key+set 1)
               k (aget s kn)]
           (when (some? k)
             (let [match? (= k max-k)
                   max-kn (if match?
                            (inc kn)
                            (let [kn (Arrays/binarySearch
                                      s
                                      (int kn)
                                      (alength s)
                                      (long max-k))]
                              (if (neg? kn)
                                (- (inc kn))
                                kn)))
                   max-k (when (< max-kn (alength s))
                           (aget s max-kn))]
               (aset key+set 0 max-kn)
               (if match?
                 (cons k (lazy-seq (self (long-mod (inc n) len) max-k)))
                 (recur (long-mod (inc n) len) max-k)))))))
     0 (first (second (aget key+sets (dec len)))))))

(defn intersect-sets-lazy [xs]
  (let [key+sets ^objects (to-array
                           (sort-by first
                                    (for [x xs]
                                      (object-array [(first x) x]))))
        len (alength key+sets)]
    ((fn self [^long n max-k]
       (when (some? max-k)
         (let [key+set ^objects (aget key+sets n)
               k (aget key+set 0)
               ^NavigableSet s (aget key+set 1)]
           (let [match? (= k max-k)
                 max-k (if match?
                         (.higher s k)
                         (.ceiling s max-k))]
             (aset key+set 0 max-k)
             (if match?
               (cons k (lazy-seq (self (long-mod (inc n) len) max-k)))
               (recur (long-mod (inc n) len) max-k))))))
     0 (first (aget key+sets (dec len))))))

(defn intersect-sets-lazy-simple-pred [xs]
  (let [[xs & rest-xs] (sort-by count xs)
        pred (apply every-pred
                    (for [xs rest-xs]
                      (fn [x]
                        (.contains ^Set xs x))))]
    (for [x xs
          :when (pred x)]
      x)))

(defn intersect-sets-stream [xs]
  (let [[xs & rest-xs] (sort-by count xs)
        pred (reduce
              (fn [^Predicate acc
                   ^Predicate x]
                (.and acc x))
              (for [xs rest-xs]
                (reify Predicate
                  (test [_ x]
                    (.contains ^Set xs x)))))]
    (.toArray (.filter (.parallelStream ^Set xs) pred))))

(defn intersect-sets-simple [xs]
  (let [xs (sort-by count xs)]
    (reduce
     (fn [^NavigableSet acc ^Set x]
       (doto acc (.retainAll x)))
     (TreeSet. ^Set (first xs))
     (rest xs))))

(defn intersect-sets-lazy-chunk [xs]
  (let [key+sets (sort-by first
                          (for [x xs]
                            [(first x) x]))
        keys (object-array (map first key+sets))
        sets (object-array (map second key+sets))
        len (count key+sets)
        chunk-size 32]
    ((fn self [cb ^long n max-k]
       (if (some? max-k)
         (let [k (aget keys n)
               ^NavigableSet s (aget sets n)]
           (let [match? (= k max-k)
                 max-k (if match?
                         (.higher s k)
                         (.ceiling s max-k))]
             (aset keys n max-k)
             (if match?
               (do (chunk-append cb k)
                   (if (= chunk-size (count cb))
                     (chunk-cons (chunk cb)
                                 (lazy-seq (self (chunk-buffer chunk-size) (long-mod (inc n) len) max-k)))
                     (recur cb (long-mod (inc n) len) max-k)))
               (recur cb (long-mod (inc n) len) max-k))))
         (chunk-cons (chunk cb) nil)))
     (chunk-buffer chunk-size) 0 (aget keys (dec len)))))

(defn intersect-sets-navigable [xs]
  (let [key+sets ^objects (to-array (sort-by first xs))
        acc (ArrayList.)
        len (alength key+sets)]
    (loop [n 0
           max-k (.first ^NavigableSet (aget key+sets (long-mod (dec n) len)))]
      (let [s ^NavigableSet (aget key+sets n)
            k (.first s)
            s ^NavigableSet (if (= k max-k)
                              (do (.add acc k)
                                  (.tailSet s k false))
                              (.tailSet s max-k true))]
        (if (.isEmpty s)
          acc
          (do (aset key+sets n s)
              (recur (long-mod (inc n) len) (.first s))))))))

(comment

  (def s (time (doto (TreeSet.)
                 (.addAll (repeatedly 1000000 #(rand-int 1000000))))))
  (def t (time (doto (TreeSet.)
                 (.addAll (repeatedly 1000000 #(rand-int 1000000))))))
  (def u (time (doto (TreeSet.)
                 (.addAll (repeatedly 1000000 #(rand-int 1000000))))))

  [(TreeSet. [0 1 3 4 5 6 7 8 9 11])
   (TreeSet. [0 2 6 7 8 9])
   (TreeSet. [2 4 5 8 10])]

  ;; Q(a, b, c) ← R(a, b), S(b, c), T (a, c).
  (let [r (tuples->tree-map
           [[1 3]
            [1 4]
            [1 5]
            [3 5]])
        s (tuples->tree-map
           [[3 4]
            [3 5]
            [4 6]
            [4 8]
            [4 9]
            [5 2]])
        t (tuples->tree-map
           [[1 4]
            [1 5]
            [1 6]
            [1 8]
            [1 9]
            [1 2]
            [3 2]])
        intersect-sets intersect-sets-lazy-chunk]
    (time
     (for [a (intersect-sets [(.keySet r)
                              (.keySet t)])
           b (intersect-sets [(.keySet ^NavigableMap (.get r a))
                              (.keySet s)])
           c (intersect-sets [(.keySet ^NavigableMap (.get s b))
                              (.keySet ^NavigableMap (.get t a))])]
       [a b c]))))
