(ns core2.operator.set
  (:require [clojure.spec.alpha :as s]
            [core2.expression.map :as emap]
            [core2.logical-plan :as lp]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.expression.map.IRelationMap
           core2.ICursor
           [core2.vector IIndirectRelation IIndirectVector]
           [java.util ArrayList HashSet LinkedList List Set]
           java.util.function.Consumer
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.ArrowBufPointer))

(defmethod lp/ra-expr :distinct [_]
  (s/cat :op #{:δ :distinct}
         :relation ::lp/ra-expression))

(defmethod lp/ra-expr :intersect [_]
  (s/cat :op #{:∩ :intersect}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :union-all [_]
  (s/cat :op #{:∪ :union-all}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :difference [_]
  (s/cat :op #{:− :except :difference}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(s/def ::incremental? boolean?)

(defmethod lp/ra-expr :fixpoint [_]
  (s/cat :op #{:μ :mu :fixpoint}
         :mu-variable ::lp/relation
         :opts (s/? (s/keys :opt-un [::incremental?]))
         :base ::lp/ra-expression
         :recursive ::lp/ra-expression))

(defmethod lp/ra-expr :relation [_]
  (s/and ::lp/relation
         (s/conformer (fn [rel]
                        {:op :relation, :relation rel})
                      :relation)))

(defmethod lp/ra-expr :assign [_]
  (s/cat :op #{:← :assign}
         :bindings (s/and vector?
                          (s/* (s/cat :variable ::lp/relation,
                                      :value ::lp/ra-expression)))
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn- ensuring-same-col-names [left-col-names right-col-names]
  (when-not (= left-col-names right-col-names)
    (throw (IllegalArgumentException. (format "union incompatible cols: %s vs %s" (pr-str left-col-names) (pr-str right-col-names)))))
  left-col-names)

(deftype UnionAllCursor [^ICursor left-cursor
                         ^ICursor right-cursor]
  ICursor
  (tryAdvance [_ c]
    (boolean
     (or (.tryAdvance left-cursor
                      (reify Consumer
                        (accept [_ in-rel]
                          (let [^IIndirectRelation in-rel in-rel]
                            (when (pos? (.rowCount in-rel))
                              (.accept c in-rel))))))

         (.tryAdvance right-cursor
                      (reify Consumer
                        (accept [_ in-rel]
                          (let [^IIndirectRelation in-rel in-rel]
                            (when (pos? (.rowCount in-rel))
                              (.accept c in-rel)))))))))

  (close [_]
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :union-all [{:keys [left right]} args]
  (lp/binary-expr left right args
    (fn [left-col-names right-col-names]
      {:col-names (ensuring-same-col-names left-col-names right-col-names)
       :->cursor (fn [_opts left-cursor right-cursor]
                   (UnionAllCursor. left-cursor right-cursor))})))

(deftype IntersectionCursor [^ICursor left-cursor
                             ^ICursor right-cursor
                             ^IRelationMap rel-map
                             difference?]
  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining right-cursor
                       (reify Consumer
                         (accept [_ in-rel]
                           (let [^IIndirectRelation in-rel in-rel
                                 builder (.buildFromRelation rel-map in-rel)]
                             (dotimes [idx (.rowCount in-rel)]
                               (.addIfNotPresent builder idx))))))

    (boolean
     (let [advanced? (boolean-array 1)]
       (while (and (not (aget advanced? 0))
                   (.tryAdvance left-cursor
                                (reify Consumer
                                  (accept [_ in-rel]
                                    (let [^IIndirectRelation in-rel in-rel
                                          row-count (.rowCount in-rel)
                                          prober (.probeFromRelation rel-map in-rel)]

                                      (when (pos? row-count)
                                        (let [idxs (IntStream/builder)]
                                          (dotimes [idx row-count]
                                            (when (cond-> (not= -1 (.indexOf prober idx))
                                                    difference? not)
                                              (.add idxs idx)))

                                          (let [idxs (.toArray (.build idxs))]
                                            (when-not (empty? idxs)
                                              (aset advanced? 0 true)
                                              (.accept c (iv/select in-rel idxs))))))))))))
       (aget advanced? 0))))

  (close [_]
    (util/try-close rel-map)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :intersect [{:keys [left right]} args]
  (lp/binary-expr left right args
    (fn [left-col-names right-col-names]
      (let [col-names (ensuring-same-col-names left-col-names right-col-names)]
        {:col-names col-names
         :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                     (IntersectionCursor. left-cursor right-cursor
                                          (emap/->relation-map allocator {:key-col-names (map name col-names)})
                                          false))}))))

(defmethod lp/emit-expr :difference [{:keys [left right]} args]
  (lp/binary-expr left right args
    (fn [left-col-names right-col-names]
      (let [col-names (ensuring-same-col-names left-col-names right-col-names)]
        {:col-names col-names
         :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                     (IntersectionCursor. left-cursor right-cursor
                                          (emap/->relation-map allocator {:key-col-names (map name col-names)})
                                          true))}))))

(deftype DistinctCursor [^ICursor in-cursor
                         ^IRelationMap rel-map]
  ICursor
  (tryAdvance [_ c]
    (let [advanced? (boolean-array 1)]
      (while (and (not (aget advanced? 0))
                  (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IIndirectRelation in-rel in-rel
                                         row-count (.rowCount in-rel)]
                                     (when (pos? row-count)
                                       (let [builder (.buildFromRelation rel-map in-rel)
                                             idxs (IntStream/builder)]
                                         (dotimes [idx row-count]
                                           (when (neg? (.addIfNotPresent builder idx))
                                             (.add idxs idx)))

                                         (let [idxs (.toArray (.build idxs))]
                                           (when-not (empty? idxs)
                                             (aset advanced? 0 true)
                                             (.accept c (iv/select in-rel idxs))))))))))))
      (aget advanced? 0)))

  (close [_]
    (util/try-close rel-map)
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :distinct [{:keys [relation]} args]
  (lp/unary-expr relation args
    (fn [inner-col-names]
      {:col-names inner-col-names
       :->cursor (fn [{:keys [allocator]} in-cursor]
                   (DistinctCursor. in-cursor (emap/->relation-map allocator {:key-col-names (map name inner-col-names)})))})))

(defn- ->set-key [^List cols ^long idx]
  (let [set-key (ArrayList. (count cols))]
    (doseq [^IIndirectVector col cols]
      (.add set-key (util/pointer-or-object (.getVector col) (.getIndex col idx))))
    set-key))

(defn- copy-set-key [^BufferAllocator allocator ^List k]
  (dotimes [n (.size k)]
    (let [x (.get k n)]
      (.set k n (util/maybe-copy-pointer allocator x))))
  k)

(defn- release-set-key [k]
  (doseq [x k
          :when (instance? ArrowBufPointer x)]
    (util/try-close (.getBuf ^ArrowBufPointer x)))
  k)

(definterface ICursorFactory
  (^core2.ICursor createCursor []))

(definterface IFixpointCursorFactory
  (^core2.ICursor createCursor [^core2.operator.set.ICursorFactory cursor-factory]))

;; https://core.ac.uk/download/pdf/11454271.pdf "Algebraic optimization of recursive queries"
;; http://webdam.inria.fr/Alice/pdfs/Chapter-14.pdf "Recursion and Negation"

(defn ->fixpoint-cursor-factory [rels incremental?]
  (reify ICursorFactory
    (createCursor [_]
      (let [rels-queue (LinkedList. rels)]
        (reify
          ICursor
          (tryAdvance [_ c]
            (if-let [rel (.poll rels-queue)]
              (do
                (.accept c rel)
                true)
              false))

          (close [_]
            (when incremental?
              (run! util/try-close rels))))))))

(deftype FixpointCursor [^BufferAllocator allocator
                         ^ICursor base-cursor
                         ^IFixpointCursorFactory recursive-cursor-factory
                         ^Set fixpoint-set
                         ^List rels
                         incremental?
                         ^:unsynchronized-mutable ^ICursor recursive-cursor
                         ^:unsynchronized-mutable continue?]
  ICursor
  (tryAdvance [this c]
    (if-not (or continue? recursive-cursor)
      false

      (let [advanced? (boolean-array 1)
            inner-c (reify Consumer
                      (accept [_ in-rel]
                        (let [^IIndirectRelation in-rel in-rel]
                          (when (pos? (.rowCount in-rel))
                            (let [cols (seq in-rel)
                                  idxs (IntStream/builder)]
                              (dotimes [idx (.rowCount in-rel)]
                                (let [k (->set-key cols idx)]
                                  (when-not (.contains fixpoint-set k)
                                    (.add fixpoint-set (copy-set-key allocator k))
                                    (.add idxs idx))))

                              (let [idxs (.toArray (.build idxs))]
                                (when-not (empty? idxs)
                                  (let [out-rel (-> (iv/select in-rel idxs)
                                                    (iv/copy allocator))]
                                    (.add rels out-rel)
                                    (.accept c out-rel)
                                    (set! (.continue? this) true)
                                    (aset advanced? 0 true)))))))))]

        (.tryAdvance base-cursor inner-c)

        (or (aget advanced? 0)
            (do
              (while (and (not (aget advanced? 0)) continue?)
                (when-let [recursive-cursor (or recursive-cursor
                                                (when continue?
                                                  (set! (.continue? this) false)
                                                  (let [cursor (.createCursor recursive-cursor-factory
                                                                              (->fixpoint-cursor-factory (vec rels) incremental?))]
                                                    (when incremental? (.clear rels))
                                                    (set! (.recursive-cursor this) cursor)
                                                    cursor)))]


                  (while (and (not (aget advanced? 0))
                              (let [more? (.tryAdvance recursive-cursor inner-c)]
                                (when-not more?
                                  (util/try-close recursive-cursor)
                                  (set! (.recursive-cursor this) nil))
                                more?)))))
              (aget advanced? 0))))))

  (close [_]
    (util/try-close recursive-cursor)
    (run! util/try-close rels)
    (run! release-set-key fixpoint-set)
    (.clear fixpoint-set)
    (util/try-close base-cursor)))

(def ^:dynamic ^:private *relation-variable->col-names* {})
(def ^:dynamic ^:private *relation-variable->cursor-factory* {})

(defmethod lp/emit-expr :relation [{:keys [relation]} _opts]
  (let [col-names (*relation-variable->col-names* relation)]
    {:col-names col-names
     :->cursor (fn [_opts]
                 (let [^ICursorFactory cursor-factory (get *relation-variable->cursor-factory* relation)]
                   (assert cursor-factory (str "can't find " relation, (pr-str *relation-variable->cursor-factory*)))
                   (.createCursor cursor-factory)))}))

(defmethod lp/emit-expr :fixpoint [{:keys [mu-variable base recursive], {:keys [incremental?]} :opts} args]
  (let [{base-col-names :col-names, ->base-cursor :->cursor} (lp/emit-expr base args)
        {recursive-col-names :col-names, ->recursive-cursor :->cursor} (binding [*relation-variable->col-names* (-> *relation-variable->col-names*
                                                                                                                    (assoc mu-variable base-col-names))]
                                                                         (lp/emit-expr recursive args))]
    {:col-names (ensuring-same-col-names base-col-names recursive-col-names)
     :->cursor
     (fn [{:keys [allocator] :as opts}]
       (FixpointCursor. allocator
                        (->base-cursor opts)
                        (reify IFixpointCursorFactory
                          (createCursor [_ cursor-factory]
                            (binding [*relation-variable->cursor-factory* (-> *relation-variable->cursor-factory*
                                                                              (assoc mu-variable cursor-factory))]
                              (->recursive-cursor opts))))
                        (HashSet.) (LinkedList.)
                        (boolean incremental?)
                        #_recursive-cursor nil
                        #_continue? true))}))

(defmethod lp/emit-expr :assign [{:keys [bindings relation]} args]
  (let [{:keys [rel-var->col-names relations]} (->> bindings
                                                     (reduce (fn [{:keys [rel-var->col-names relations]} {:keys [variable value]}]
                                                               (binding [*relation-variable->col-names* rel-var->col-names]
                                                                 (let [{:keys [col-names ->cursor]} (lp/emit-expr value args)]
                                                                   {:rel-var->col-names (-> rel-var->col-names
                                                                                            (assoc variable col-names))

                                                                    :relations (conj relations {:variable variable, :->cursor ->cursor})})))
                                                             {:rel-var->col-names *relation-variable->col-names*
                                                              :relations []}))
        {:keys [col-names ->cursor]} (binding [*relation-variable->col-names* rel-var->col-names]
                                       (lp/emit-expr relation args))]
    {:col-names col-names
     :->cursor (fn [opts]
                 (let [rel-var->cursor-factory (->> relations
                                                    (reduce (fn [acc {:keys [variable ->cursor]}]
                                                              (-> acc
                                                                  (assoc variable (reify ICursorFactory
                                                                                    (createCursor [_]
                                                                                      (binding [*relation-variable->cursor-factory* acc]
                                                                                        (->cursor opts)))))))
                                                            *relation-variable->cursor-factory*))]
                   (binding [*relation-variable->cursor-factory* rel-var->cursor-factory]
                     (->cursor opts))))}))
