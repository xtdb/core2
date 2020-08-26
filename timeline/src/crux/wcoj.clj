(ns crux.wcoj
  (:import [java.util Arrays ArrayList NavigableMap NavigableSet Set TreeMap TreeSet]
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
  (let [xs (sort-by first xs)
        keys (object-array (map first xs))
        sets (object-array xs)
        len (count xs)
        acc (ArrayList.)]
    (loop [n 0
           max-k (aget keys (long-mod (dec n) len))]
      (let [k (aget keys n)
            ^NavigableSet s (aget sets n)]
        (if-let [max-k (if (= k max-k)
                         (do (.add acc k)
                             (.higher s k))
                         (.ceiling s max-k))]
          (do (aset keys n max-k)
              (recur (long-mod (inc n) len) max-k))
          acc)))))

(defn intersect-sets-arrays [xs]
  (let [xs (sort-by first xs)
        keys (int-array (count xs))
        sets (object-array (map to-array xs))
        len (count xs)]
    ((fn self [^long n max-k]
       (when (some? max-k)
         (let [kn (aget keys n)
               s ^objects (aget sets n)
               k (aget s kn)]
           (when (some? k)
             (let [match? (= k max-k)
                   max-kn (if match?
                            (inc kn)
                            (let [kn (Arrays/binarySearch
                                      s
                                      kn
                                      (alength s)
                                      max-k)]
                              (if (neg? kn)
                                (- (inc kn))
                                kn)))
                   max-k (when (< max-kn (alength s))
                           (aget s max-kn))]
               (aset keys n (int max-kn))
               (if match?
                 (cons k (lazy-seq (self (long-mod (inc n) len) max-k)))
                 (recur (long-mod (inc n) len) max-k)))))))
     0 (first (aget sets (dec len))))))

(defn intersect-sets-lazy [xs]
  (let [xs (sort-by first xs)
        keys (object-array (map first xs))
        sets (object-array xs)
        len (count xs)]
    ((fn self [^long n max-k]
       (when (some? max-k)
         (let [k (aget keys n)
               ^NavigableSet s (aget sets n)]
           (let [match? (= k max-k)
                 max-k (if match?
                         (.higher s k)
                         (.ceiling s max-k))]
             (aset keys n max-k)
             (if match?
               (cons k (lazy-seq (self (long-mod (inc n) len) max-k)))
               (recur (long-mod (inc n) len) max-k))))))
     0 (first (aget keys (dec len))))))

(defn intersect-sets-lazy-simple-pred [xs]
  (let [[xs & rest-xs] (sort-by count xs)
        pred (apply every-pred
                    (for [xs rest-xs]
                      (fn [x]
                        (.contains ^Set xs x))))]
    (for [x xs
          :when (pred x)]
      x)))

(defn intersect-sets-stream ^java.util.stream.Stream [xs]
  (let [[xs & rest-xs] (sort-by count xs)
        pred (reduce
              (fn [^Predicate acc
                   ^Predicate x]
                (.and acc x))
              (for [xs rest-xs]
                (reify Predicate
                  (test [_ x]
                    (.contains ^Set xs x)))))]
    (.filter (.parallelStream ^Set xs) pred)))

(defn intersect-sets-simple [xs]
  (let [xs (sort-by count xs)]
    (reduce
     (fn [^NavigableSet acc ^Set x]
       (doto acc (.retainAll x)))
     (TreeSet. ^Set (first xs))
     (rest xs))))

(defn intersect-sets-lazy-chunk [xs]
  (let [xs (sort-by first xs)
        keys (object-array (map first xs))
        sets (object-array xs)
        len (count xs)
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
  (let [sets ^objects (to-array (sort-by first xs))
        acc (ArrayList.)
        len (alength sets)]
    (loop [n 0
           max-k (.first ^NavigableSet (aget sets (long-mod (dec n) len)))]
      (let [s ^NavigableSet (aget sets n)
            k (.first s)
            s ^NavigableSet (if (= k max-k)
                              (do (.add acc k)
                                  (.tailSet s k false))
                              (.tailSet s max-k true))]
        (if (.isEmpty s)
          acc
          (do (aset sets n s)
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
     (seq (.toArray (.flatMap (intersect-sets-stream [(.keySet r)
                                                      (.keySet t)])
                              (reify Function
                                (apply [_ a]
                                  (.flatMap (intersect-sets-stream [(.keySet ^NavigableMap (.get r a))
                                                                    (.keySet s)])
                                            (reify Function
                                              (apply [_ b]
                                                (.flatMap (intersect-sets-stream [(.keySet ^NavigableMap (.get s b))
                                                                                  (.keySet ^NavigableMap (.get t a))])
                                                          (reify Function
                                                            (apply [_ c]
                                                              (java.util.stream.Stream/of [a b c])))))))))))))
    (time
     (for [a (intersect-sets [(.keySet r)
                              (.keySet t)])
           b (intersect-sets [(.keySet ^NavigableMap (.get r a))
                              (.keySet s)])
           c (intersect-sets [(.keySet ^NavigableMap (.get s b))
                              (.keySet ^NavigableMap (.get t a))])]
       [a b c]))))
