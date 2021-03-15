(ns core2.temporal.kd-tree
  (:import [java.util ArrayDeque ArrayList Arrays Collection Comparator Date Deque HashMap List Random Spliterator Spliterator$OfInt Spliterators]
           [java.util.function Consumer IntConsumer Function Predicate]
           [java.util.stream Collectors StreamSupport]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol KdTree
  (kd-tree-insert [_ location])
  (kd-tree-delete [_ location])
  (kd-tree-range-search [_ min-range max-range])
  (kd-tree-depth-first [_]))

(defrecord Node [^longs location left right deleted?])

(defmacro ^:private next-axis [axis k]
  `(let [next-axis# (unchecked-inc-int ~axis)]
     (if (= ~k next-axis#)
       (int 0)
       next-axis#)))

(def ^:private ^Class longs-class (Class/forName "[J"))

(defn- ->longs ^longs [xs]
  (if (instance? longs-class xs)
    xs
    (long-array xs)))

(extend-protocol KdTree
  nil
  (kd-tree-insert [_ location]
    (Node. (->longs location) nil nil false))
  (kd-tree-delete [_ _])
  (kd-tree-range-search [_ _ _]
    (Spliterators/emptySpliterator))
  (kd-tree-depth-first [_ _ _]
    (Spliterators/emptySpliterator)))

(defn ->node-kd-tree
  (^core2.temporal.kd_tree.Node [points]
   (->node-kd-tree (mapv ->longs points) 0))
  (^core2.temporal.kd_tree.Node [points ^long axis]
   (when-let [points (not-empty points)]
     (let [k (alength ^longs (first points))
           points (vec (sort-by #(aget ^longs % axis) points))
           median (quot (count points) 2)
           axis (next-axis axis k)]
       (->Node (nth points median)
               (->node-kd-tree (subvec points 0 median) axis)
               (->node-kd-tree (subvec points (inc median)) axis)
               false)))))

(defn node-kd-tree->seq [^Node kd-tree]
  (iterator-seq (Spliterators/iterator ^Spliterator (kd-tree-depth-first kd-tree))))

(deftype NodeStackEntry [^Node node ^int axis])

(defmacro ^:private in-range? [mins xs maxs]
  `(let [mins# ~mins
         xs# ~xs
         maxs# ~maxs
         len# (alength xs#)]
     (loop [n# (int 0)]
       (if (= n# len#)
         true
         (let [x# (aget xs# n#)]
           (if (and (<= (aget mins# n#) x#)
                    (<= x# (aget maxs# n#)))
             (recur (inc n#))
             false))))))

(deftype NodeRangeSearchSpliterator [^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator
  (tryAdvance [_ c]
    (loop []
      (if-let [^NodeStackEntry entry (.poll stack)]
        (let [^Node node (.node entry)
              axis (.axis entry)
              ^longs location (.location node)
              location-axis (aget location axis)
              min-match? (<= (aget min-range axis) location-axis)
              max-match? (<= location-axis (aget max-range axis))
              axis (next-axis axis k)]
          (when-let [right (when max-match?
                             (.right node))]
            (.push stack (NodeStackEntry. right axis)))
          (when-let [left (when min-match?
                            (.left node))]
            (.push stack (NodeStackEntry. left axis)))

          (if (and min-match?
                   max-match?
                   (not (.deleted? node))
                   (in-range? min-range location max-range))
            (do (.accept c location)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

(extend-protocol KdTree
  Node
  (kd-tree-insert [kd-tree location]
    (let [location (->longs location)
          k (alength location)]
      (loop [axis 0
             node kd-tree
             build-fn identity]
        (if-not node
          (build-fn (Node. location nil nil false))
          (let [^longs location-node (.location node)
                location-axis (aget location-node axis)]
            (cond
              (Arrays/equals location location-node)
              (build-fn node)

              (< (aget location axis) location-axis)
              (recur (next-axis axis k) (.left node) (comp build-fn (partial assoc node :left)))

              (<= location-axis (aget location axis))
              (recur (next-axis axis k) (.right node) (comp build-fn (partial assoc node :right)))))))))

  (kd-tree-delete [kd-tree location]
    (let [location (->longs location)
          k (alength location)]
      (loop [axis 0
             node kd-tree
             build-fn identity]
        (if-not node
          (build-fn nil)
          (let [^longs location-node (.location node)
                location-axis (aget location-node axis)]
            (cond
              (Arrays/equals location location-node)
              (build-fn (assoc node :deleted? true))

              (< (aget location axis) location-axis)
              (recur (next-axis axis k) (.left node) (comp build-fn (partial assoc node :left)))

              (<= location-axis (aget location axis))
              (recur (next-axis axis k) (.right node) (comp build-fn (partial assoc node :right)))))))))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          k (count (some-> kd-tree (.location)))
          stack (doto (ArrayDeque.)
                  (.push (NodeStackEntry. kd-tree 0)))]
      (->NodeRangeSearchSpliterator min-range max-range k stack)))

  (kd-tree-depth-first [kd-tree]
    (let [k (count (some-> kd-tree (.location)))]
      (kd-tree-range-search kd-tree (repeat k Long/MIN_VALUE) (repeat k Long/MAX_VALUE)))))

(deftype ColumnStackEntry [^int start ^int end ^int axis])

(defn ->column-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator points]
  (let [points (object-array (mapv ->longs points))
        n (alength points)
        k (alength ^longs (aget points 0))
        columns (VectorSchemaRoot. ^List (repeatedly k #(doto (BigIntVector. "" allocator)
                                                          (.setInitialCapacity n)
                                                          (.allocateNew))))
        stack (doto (ArrayDeque.)
                (.push (ColumnStackEntry. 0 (alength points) 0)))]
    (loop []
      (when-let [^ColumnStackEntry entry (.poll stack)]
        (let [start (.start entry)
              end (.end entry)
              axis (.axis entry)]
          (Arrays/sort points start end (reify Comparator
                                          (compare [_ x y]
                                            (Long/compare (aget ^longs x axis)
                                                          (aget ^longs y axis)))))
          (let [median (quot (+ start end) 2)
                axis (next-axis axis k)]
            (when (< (inc median) end)
              (.push stack (ColumnStackEntry. (inc median) end axis)))
            (when (< start median)
              (.push stack (ColumnStackEntry. start median axis)))))
        (recur)))
    (dotimes [n k]
      (let [^BigIntVector v (.getVector columns n)]
        (dotimes [m (alength points)]
          (.set v m (aget ^longs (aget points m) n)))))
    (doto columns
      (.setRowCount n))))

(defmacro ^:private in-range-column? [mins xs k idx maxs]
  (let [col-sym (with-meta (gensym "col") {:tag `BigIntVector})]
    `(let [idx# ~idx
           mins# ~mins
           xs# ~xs
           maxs# ~maxs
           len# ~k]
       (loop [n# (int 0)]
         (if (= n# len#)
           true
           (let [~col-sym (.getVector xs# n#)
                 x# (.get ~col-sym idx#)]
             (if (and (<= (aget mins# n#) x#)
                      (<= x# (aget maxs# n#)))
               (recur (inc n#))
               false)))))))

(deftype ColumnRangeSearchSpliterator [^VectorSchemaRoot kd-tree ^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator$OfInt
  (^boolean tryAdvance [_ ^IntConsumer c]
    (loop []
      (if-let [^ColumnStackEntry entry (.poll stack)]
        (let [start (.start entry)
              end (.end entry)
              axis (.axis entry)
              median (quot (+ start end) 2)
              ^BigIntVector axis-column (.getVector kd-tree axis)
              axis-value (.get axis-column median)
              min-match? (<= (aget min-range axis) axis-value)
              max-match? (<= axis-value (aget max-range axis))
              axis (next-axis axis k)]

          (when (and max-match? (< (inc median) end))
            (.push stack (ColumnStackEntry. (inc median) end axis)))
          (when (and min-match? (< start median))
            (.push stack (ColumnStackEntry. start median axis)))

          (if (and min-match?
                   max-match?
                   (in-range-column? min-range kd-tree k median max-range))
            (do (.accept c median)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

(extend-protocol KdTree
  VectorSchemaRoot
  (kd-tree-insert [_ location]
    (throw (UnsupportedOperationException.)))

  (kd-tree-delete [_ location]
    (throw (UnsupportedOperationException.)))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          k (alength min-range)
          stack (doto (ArrayDeque.)
                  (.push (ColumnStackEntry. 0 (.getRowCount kd-tree) 0)))]
      (->ColumnRangeSearchSpliterator kd-tree min-range max-range k stack)))

  (column-kd-tree-depth-first [kd-tree]
    (let [k (.size (.getFields (.getSchema kd-tree)))]
      (kd-tree-range-search kd-tree (repeat k Long/MIN_VALUE) (repeat k Long/MAX_VALUE)))))

;; TODO:
;; Sanity check counts via stream count.
;; Try different implicit orders for implicit/column/ist.
(defn- run-test []
  (with-open [allocator (RootAllocator.)]
    (assert (= (-> (->node-kd-tree [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])
                   (kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/stream false)
                   (.toArray)
                   (->> (mapv vec)))

               (-> (reduce
                    kd-tree-insert
                    nil
                    [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])
                   (kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/stream false)
                   (.toArray)
                   (->> (mapv vec)))

               (with-open [column-kd-tree (->column-kd-tree allocator [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])]
                 (-> column-kd-tree
                     (kd-tree-range-search [0 0] [8 4])
                     (StreamSupport/intStream false)
                     (.toArray)
                     (->> (mapv #(mapv (fn [^BigIntVector col]
                                         (.get col %))
                                       (.getFieldVectors column-kd-tree)))))))
            "wikipedia-test"))

  (with-open [allocator (RootAllocator.)]
    (doseq [k (range 2 4)]
      (let [rng (Random. 0)
            _ (prn :k k)
            ns 100000
            qs 10000
            ts 3
            _ (prn :gen-points ns)
            points (time
                    (vec (for [n (range ns)]
                           (long-array (repeatedly k #(.nextLong rng))))))

            _ (prn :gen-queries qs)
            queries (time
                     (vec (for [n (range qs)
                                :let [min+max-pairs (repeatedly k #(sort [(.nextLong rng)
                                                                          (.nextLong rng)]))]]
                            [(long-array (map first min+max-pairs))
                             (long-array (map second min+max-pairs))])))]

        (prn :range-queries-scan qs)
        (time
         (doseq [[^longs min-range ^longs max-range] queries]
           (-> (.stream ^Collection points)
               (.filter (reify Predicate
                          (test [_ location]
                            (in-range? min-range ^longs location max-range))))
               (.count))))

        (prn :build-kd-tree-insert ns)
        (let [kd-tree (time
                       (reduce
                        kd-tree-insert
                        nil
                        points))]

          (prn :range-queries-kd-tree-insert qs)
          (dotimes [_ ts]
            (time
             (doseq [[min-range max-range] queries]
               (-> (kd-tree-range-search kd-tree min-range max-range)
                   (StreamSupport/stream false)
                   (.count))))))


        (prn :build-kd-tree-bulk ns)
        (let [kd-tree (time
                       (->node-kd-tree points))]

          (prn :range-queries-kd-tree-bulk qs)
          (dotimes [_ ts]
            (time
             (doseq [[min-range max-range] queries]
               (-> (kd-tree-range-search kd-tree min-range max-range)
                   (StreamSupport/stream false)
                   (.count))))))

        (prn :build-column-kd-tree ns)
        (with-open [kd-tree (time
                             (->column-kd-tree allocator points))]
          (prn :range-queries-column-kd-tree qs)
          (dotimes [_ ts]
            (time
             (doseq [[min-range max-range] queries]
               (-> (kd-tree-range-search kd-tree min-range max-range)
                   (StreamSupport/intStream false)
                   (.count))))))))))

;; Bitemporal Spike

(def end-of-time #inst "9999-12-31T23:59:59.000Z")

(defn ->location ^longs [{:keys [id row-id tt-start tt-end vt-start vt-end]
                          :or {vt-start tt-start
                               tt-end end-of-time
                               vt-end end-of-time}}]
  (long-array [id
               row-id
               (if (inst? vt-start)
                 (inst-ms vt-start)
                 vt-start)
               (if (inst? vt-end)
                 (inst-ms vt-end)
                 vt-end)
               (if (inst? tt-start)
                 (inst-ms tt-start)
                 tt-start)
               (if (inst? tt-end)
                 (inst-ms tt-end)
                 tt-end)]))

(defn ->min-range ^longs [{:keys [id row-id tt-start tt-end vt-start vt-end]}]
  (long-array [(or id Long/MIN_VALUE)
               (or row-id Long/MIN_VALUE)
               (if vt-start
                 (inst-ms vt-start)
                 Long/MIN_VALUE)
               (if vt-end
                 (inst-ms vt-end)
                 Long/MIN_VALUE)
               (if tt-start
                 (inst-ms tt-start)
                 Long/MIN_VALUE)
               (if tt-end
                 (inst-ms tt-end)
                 Long/MIN_VALUE)]))

(defn ->max-range ^longs [{:keys [id row-id tt-start tt-end vt-start vt-end]}]
  (long-array [(or id Long/MAX_VALUE)
               (or row-id Long/MAX_VALUE)
               (if vt-start
                 (inst-ms vt-start)
                 Long/MAX_VALUE)
               (if vt-end
                 (inst-ms vt-end)
                 Long/MAX_VALUE)
               (if tt-start
                 (inst-ms tt-start)
                 Long/MAX_VALUE)
               (if tt-end
                 (inst-ms tt-end)
                 Long/MAX_VALUE)]))

(defn ->coordinate [^longs location]
  (let [[id row-id vt-start vt-end tt-start tt-end] location]
    (zipmap [:id :row-id :vt-start :vt-end :tt-start :tt-end]
            [id row-id  (Date. ^long vt-start) (Date. ^long vt-end) (Date. ^long tt-start) (Date. ^long tt-end)])))

(defn put-entity [kd-tree {:keys [id tt-start vt-start vt-end tombstone?] :as coordinates
                           :or {vt-start tt-start
                                vt-end end-of-time}}]
  (let [^Spliterator id-overlap (kd-tree-range-search kd-tree
                                                      (->min-range {:id id :vt-end vt-start :tt-end end-of-time})
                                                      (->max-range {:id id :vt-start (Date. (dec ^long (inst-ms vt-end))) :tt-end end-of-time}))
        updates (->> (for [coord (map #(zipmap [:id :row-id :vt-start :vt-end :tt-start :tt-end] %)
                                      (iterator-seq (Spliterators/iterator id-overlap)))]
                       (cond-> [(assoc coord :tt-end (inst-ms tt-start))
                                (assoc coord :tombstone? true)]
                         (< ^long (:vt-start coord) ^long (inst-ms vt-start))
                         (conj (assoc coord :tt-start (inst-ms tt-start) :vt-end (inst-ms vt-start)))

                         (> ^long (:vt-end coord) ^long (inst-ms vt-end))
                         (conj (assoc coord :tt-start (inst-ms tt-start) :vt-start (inst-ms vt-end)))))
                     (reduce into (if tombstone?
                                    #{}
                                    #{coordinates})))]
    (reduce
     kd-tree-insert
     (reduce
      kd-tree-delete
      kd-tree
      (map ->location (filter :tombstone? updates)))
     (map ->location (remove :tombstone? updates)))))


(defn- temporal-rows [kd-tree row-id->row]
  (vec (for [{:keys [row-id] :as row} (sort-by (juxt :tt-start :row-id) (map ->coordinate (node-kd-tree->seq kd-tree)))]
         (merge row (get row-id->row row-id)))))

;; NOTE: This is from the bitemporal chapter in Snodgrass book.
(defn- run-bitemp-test []
  (let [kd-tree nil
        row-id->row (HashMap.)]
    ;; Eva Nielsen buys the flat at Skovvej 30 in Aalborg on January 10,
    ;; 1998.
    (let [kd-tree (put-entity kd-tree {:id 7797
                                       :row-id 1
                                       :tt-start #inst "1998-01-10"})]
      (.put row-id->row 1 {:customer-number 145})
      (assert (= [{:id 7797,
                   :customer-number 145,
                   :row-id 1,
                   :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                   :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                   :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                   :tt-end #inst "9999-12-31T23:59:59.000-00:00"}]
                 (temporal-rows kd-tree row-id->row)))

      ;; Peter Olsen buys the flat on January 15, 1998.
      (let [kd-tree (put-entity kd-tree {:id 7797
                                         :row-id 2
                                         :tt-start #inst "1998-01-15"})]
        (.put row-id->row 2 {:customer-number 827})
        (assert (= [{:id 7797,
                     :row-id 1,
                     :customer-number 145,
                     :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                     :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                     :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                     :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                    {:id 7797,
                     :customer-number 145,
                     :row-id 1,
                     :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                     :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                     :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                     :tt-end #inst "9999-12-31T23:59:59.000-00:00"}
                    {:id 7797,
                     :row-id 2,
                     :customer-number 827,
                     :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                     :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                     :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                     :tt-end #inst "9999-12-31T23:59:59.000-00:00"}]
                   (temporal-rows kd-tree row-id->row)))

        ;; Peter Olsen sells the flat on January 20, 1998.
        (let [kd-tree (put-entity kd-tree {:id 7797
                                           :row-id 3
                                           :tt-start #inst "1998-01-20"
                                           :tombstone? true})]
          (.put row-id->row 3 {:customer-number 827})
          (assert (= [{:id 7797,
                       :customer-number 145,
                       :row-id 1,
                       :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                       :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                       :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                       :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                      {:id 7797,
                       :customer-number 145,
                       :row-id 1,
                       :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                       :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                       :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                       :tt-end #inst "9999-12-31T23:59:59.000-00:00"}
                      {:id 7797,
                       :customer-number 827,
                       :row-id 2,
                       :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                       :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                       :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                       :tt-end #inst "1998-01-20T00:00:00.000-00:00"}
                      {:id 7797,
                       :customer-number 827,
                       :row-id 2,
                       :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                       :vt-end #inst "1998-01-20T00:00:00.000-00:00",
                       :tt-start #inst "1998-01-20T00:00:00.000-00:00",
                       :tt-end #inst "9999-12-31T23:59:59.000-00:00"}]
                     (temporal-rows kd-tree row-id->row)))

          ;; Eva actually purchased the flat on January 3, performed on January 23.
          (let [kd-tree (put-entity kd-tree {:id 7797
                                             :row-id 4
                                             :tt-start #inst "1998-01-23"
                                             :vt-start #inst "1998-01-03"
                                             :vt-end #inst "1998-01-15"})]
            (.put row-id->row 4 {:customer-number 145})
            (assert (= [{:id 7797,
                         :customer-number 145,
                         :row-id 1,
                         :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                         :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                         :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                         :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                        {:id 7797,
                         :customer-number 145,
                         :row-id 1,
                         :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                         :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-end #inst "1998-01-23T00:00:00.000-00:00"}
                        {:id 7797,
                         :customer-number 827,
                         :row-id 2,
                         :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                         :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                         :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-end #inst "1998-01-20T00:00:00.000-00:00"}
                        {:id 7797,
                         :row-id 2,
                         :customer-number 827,
                         :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                         :vt-end #inst "1998-01-20T00:00:00.000-00:00",
                         :tt-start #inst "1998-01-20T00:00:00.000-00:00",
                         :tt-end #inst "9999-12-31T23:59:59.000-00:00"}
                        {:id 7797,
                         :customer-number 145,
                         :row-id 4,
                         :vt-start #inst "1998-01-03T00:00:00.000-00:00",
                         :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-start #inst "1998-01-23T00:00:00.000-00:00",
                         :tt-end #inst "9999-12-31T23:59:59.000-00:00"}]
                       (temporal-rows kd-tree row-id->row)))

            ;; TODO: the assertions below aren't validated yet.

            ;; A sequenced deletion performed on January 26: Eva actually purchased the flat on January 5.
            (let [kd-tree (put-entity kd-tree {:id 7797
                                               :row-id 5
                                               :tt-start #inst "1998-01-26"
                                               :vt-start #inst "1998-01-03"
                                               :vt-end #inst "1998-01-05"
                                               :tombstone? true})]
              (.put row-id->row 5 {:customer-number 145})
              (assert (= [{:id 7797,
                           :customer-number 145,
                           :row-id 1,
                           :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                           :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                           :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 145,
                           :row-id 1,
                           :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-23T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 827,
                           :row-id 2,
                           :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                           :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                           :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-20T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 827,
                           :row-id 2,
                           :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-20T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-20T00:00:00.000-00:00",
                           :tt-end #inst "9999-12-31T23:59:59.000-00:00"}
                          {:id 7797,
                           :customer-number 145,
                           :row-id 4,
                           :vt-start #inst "1998-01-03T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-23T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-26T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 145,
                           :row-id 4,
                           :vt-start #inst "1998-01-05T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-26T00:00:00.000-00:00",
                           :tt-end #inst "9999-12-31T23:59:59.000-00:00"}]
                         (temporal-rows kd-tree row-id->row)))

              ;; A sequenced update performed on January 28: Peter actually purchased the flat on January 12.
              (let [kd-tree (put-entity kd-tree {:id 7797
                                                 :row-id 6
                                                 :tt-start #inst "1998-01-28"
                                                 :vt-start #inst "1998-01-12"})]
                (.put row-id->row 6 {:customer-number 827})
                (assert (= [{:id 7797,
                             :customer-number 145,
                             :row-id 1,
                             :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                             :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                             :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                             :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                            {:id 7797,
                             :customer-number 145,
                             :row-id 1,
                             :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                             :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                             :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                             :tt-end #inst "1998-01-23T00:00:00.000-00:00"}
                            {:id 7797,
                             :customer-number 827,
                             :row-id 2,
                             :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                             :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                             :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                             :tt-end #inst "1998-01-20T00:00:00.000-00:00"}
                            {:id 7797,
                             :customer-number 827,
                             :row-id 2,
                             :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                             :vt-end #inst "1998-01-20T00:00:00.000-00:00",
                             :tt-start #inst "1998-01-20T00:00:00.000-00:00",
                             :tt-end #inst "1998-01-28T00:00:00.000-00:00"}
                            {:id 7797,
                             :customer-number 145,
                             :row-id 4,
                             :vt-start #inst "1998-01-03T00:00:00.000-00:00",
                             :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                             :tt-start #inst "1998-01-23T00:00:00.000-00:00",
                             :tt-end #inst "1998-01-26T00:00:00.000-00:00"}
                            {:id 7797,
                             :customer-number 145,
                             :row-id 4,
                             :vt-start #inst "1998-01-05T00:00:00.000-00:00",
                             :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                             :tt-start #inst "1998-01-26T00:00:00.000-00:00",
                             :tt-end #inst "1998-01-28T00:00:00.000-00:00"}
                            {:id 7797,
                             :customer-number 145,
                             :row-id 4,
                             :vt-start #inst "1998-01-05T00:00:00.000-00:00",
                             :vt-end #inst "1998-01-12T00:00:00.000-00:00",
                             :tt-start #inst "1998-01-28T00:00:00.000-00:00",
                             :tt-end #inst "9999-12-31T23:59:59.000-00:00"}
                            {:id 7797,
                             :customer-number 827,
                             :row-id 6,
                             :vt-start #inst "1998-01-12T00:00:00.000-00:00",
                             :vt-end #inst "9999-12-31T23:59:59.000-00:00",
                             :tt-start #inst "1998-01-28T00:00:00.000-00:00",
                             :tt-end #inst "9999-12-31T23:59:59.000-00:00"}]
                           (temporal-rows kd-tree row-id->row)))))))))))
