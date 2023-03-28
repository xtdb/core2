(ns core2.operator.apply
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.logical-plan :as lp]
            [core2.rewrite :refer [zmatch]]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           (core2 ICursor)
           (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           (java.util.function Consumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BitVector NullVector)))

(defmethod lp/ra-expr :apply [_]
  (s/cat :op #{:apply}
         :mode (s/or :mark-join (s/map-of #{:mark-join} ::lp/column-expression, :count 1, :conform-keys true)
                     :otherwise #{:cross-join, :left-outer-join, :semi-join, :anti-join, :single-join})
         :columns (s/map-of ::lp/column ::lp/column, :conform-keys true)
         :independent-relation ::lp/ra-expression
         :dependent-relation ::lp/ra-expression))

(definterface IDependentCursorFactory
  (^core2.ICursor openDependentCursor [^core2.vector.IIndirectRelation inRelation, ^int idx]))

(definterface ModeStrategy
  (^void accept [^core2.ICursor dependentCursor
                 ^core2.vector.IRelationWriter dependentOutWriter
                 ^java.util.stream.IntStream$Builder idxs
                 ^int inIdx]))

(defn ->mode-strategy [mode dependent-col-types]
  (zmatch mode
    [:mark-join mark-spec]
    (let [[col-name _expr] (first (:mark-join mark-spec))]
      (reify ModeStrategy
        (accept [_ dep-cursor dep-out-writer idxs in-idx]
          (let [out-writer (.writerForName dep-out-writer (name col-name) [:union #{:null :bool}])
                ^BitVector out-vec (.getVector out-writer)]
            (.add idxs in-idx)
            (let [!match (int-array [-1])]
              (while (and (not (== 1 (aget !match 0)))
                          (.tryAdvance dep-cursor
                                       (reify Consumer
                                         (accept [_ dep-rel]
                                           (let [match-vec (.vectorForName ^IIndirectRelation dep-rel "_expr")
                                                 match-rdr (.polyReader match-vec [:union #{:null :bool}])]
                                             ;; HACK: if the iteration order changes I'd like to know about it :)
                                             (case (vec #{:null :bool})
                                               [:null :bool]
                                               (dotimes [idx (.getValueCount match-vec)]
                                                 (case (.read match-rdr idx)
                                                   0 (aset !match 0 (max (aget !match 0) 0))
                                                   1 (aset !match 0 (max (aget !match 0) (if (.readBoolean match-rdr) 1 -1))))))))))))
              (let [match (aget !match 0)]
                (.startValue out-writer)
                (if (zero? match)
                  (.setNull out-vec in-idx)
                  (.setSafe out-vec in-idx (case match 1 1, -1 0)))
                (.endValue out-writer)))))))

    [:otherwise simple-mode]
    (case simple-mode
      :cross-join
      (reify ModeStrategy
        (accept [_ dep-cursor dep-out-writer idxs in-idx]
          (doseq [[col-name col-type] dependent-col-types]
            (.writerForName dep-out-writer (name col-name) col-type))

          (.forEachRemaining dep-cursor
                             (reify Consumer
                               (accept [_ dep-rel]
                                 (let [^IIndirectRelation dep-rel dep-rel]
                                   (vw/append-rel dep-out-writer dep-rel)

                                   (dotimes [_ (.rowCount dep-rel)]
                                     (.add idxs in-idx))))))))

      :left-outer-join
      (reify ModeStrategy
        (accept [_ dep-cursor dep-out-writer idxs in-idx]
          (doseq [[col-name col-type] dependent-col-types]
            (.writerForName dep-out-writer (name col-name) col-type))

          (let [match? (boolean-array [false])]
            (.forEachRemaining dep-cursor
                               (reify Consumer
                                 (accept [_ dep-rel]
                                   (let [^IIndirectRelation dep-rel dep-rel]
                                     (when (pos? (.rowCount dep-rel))
                                       (aset match? 0 true)
                                       (vw/append-rel dep-out-writer dep-rel)

                                       (dotimes [_ (.rowCount dep-rel)]
                                         (.add idxs in-idx)))))))

            (when-not (aget match? 0)
              (.add idxs in-idx)
              (doseq [[col-name col-type] dependent-col-types]
                (vw/append-vec (.writerForName dep-out-writer (name col-name) col-type)
                               (iv/->direct-vec (doto (NullVector. (name col-name))
                                                  (.setValueCount 1)))))))))

      :semi-join
      (reify ModeStrategy
        (accept [_ dep-cursor _dep-out-writer idxs in-idx]
          (let [match? (boolean-array [false])]
            (while (and (not (aget match? 0))
                        (.tryAdvance dep-cursor
                                     (reify Consumer
                                       (accept [_ dep-rel]
                                         (let [^IIndirectRelation dep-rel dep-rel]
                                           (when (pos? (.rowCount dep-rel))
                                             (aset match? 0 true)
                                             (.add idxs in-idx)))))))))))

      :anti-join
      (reify ModeStrategy
        (accept [_ dep-cursor _dep-out-writer idxs in-idx]
          (let [match? (boolean-array [false])]
            (while (and (not (aget match? 0))
                        (.tryAdvance dep-cursor
                                     (reify Consumer
                                       (accept [_ dep-rel]
                                         (let [^IIndirectRelation dep-rel dep-rel]
                                           (when (pos? (.rowCount dep-rel))
                                             (aset match? 0 true))))))))
            (when-not (aget match? 0)
              (.add idxs in-idx)))))

      :single-join
      (reify ModeStrategy
        (accept [_ dep-cursor dep-out-writer idxs in-idx]
          (doseq [[col-name col-type] dependent-col-types]
            (.writerForName dep-out-writer (name col-name) col-type))

          (let [match? (boolean-array [false])]
            (.forEachRemaining dep-cursor
                               (reify Consumer
                                 (accept [_ dep-rel]
                                   (let [^IIndirectRelation dep-rel dep-rel
                                         row-count (.rowCount dep-rel)]
                                     (cond
                                       (zero? row-count) nil

                                       (> (+ row-count (if (aget match? 0) 1 0)) 1)
                                       (throw (err/runtime-err :core2.single-join/cardinality-violation
                                                               {::err/message "cardinality violation"}))

                                       :else
                                       (do
                                         (aset match? 0 true)
                                         (.add idxs in-idx)
                                         (vw/append-rel dep-out-writer dep-rel)))))))

            (when-not (aget match? 0)
              (.add idxs in-idx)
              (doseq [[col-name col-type] dependent-col-types]
                (vw/append-vec (.writerForName dep-out-writer (name col-name) col-type)
                               (iv/->direct-vec (doto (NullVector. (name col-name))
                                                  (.setValueCount 1))))))))))))

(deftype ApplyCursor [^BufferAllocator allocator
                      ^ModeStrategy mode-strategy
                      ^ICursor independent-cursor
                      ^IDependentCursorFactory dependent-cursor-factory]
  ICursor
  (tryAdvance [_ c]
    (.tryAdvance independent-cursor
                 (reify Consumer
                   (accept [_ in-rel]
                     (let [^IIndirectRelation in-rel in-rel
                           idxs (IntStream/builder)]

                       (with-open [dep-out-writer (vw/->rel-writer allocator)]
                         (dotimes [in-idx (.rowCount in-rel)]
                           (with-open [dep-cursor (.openDependentCursor dependent-cursor-factory
                                                                        in-rel in-idx)]
                             (.accept mode-strategy dep-cursor dep-out-writer idxs in-idx)))

                         (let [idxs (.toArray (.build idxs))
                               out-row-count (alength idxs)]

                           (.accept c (iv/->indirect-rel (concat (for [^IIndirectVector col in-rel]
                                                                   (.select col idxs))
                                                                 (for [^IVectorWriter vec-writer dep-out-writer]
                                                                   (-> (.getVector vec-writer)
                                                                       (doto (.setValueCount out-row-count))
                                                                       iv/->direct-vec))))))))))))

  (close [_]
    (util/try-close independent-cursor)))

(defmethod lp/emit-expr :apply [{:keys [mode columns independent-relation dependent-relation]} args]

  ;; TODO: decodes/re-encodes row values - can we pass these directly to the sub-query?

  (lp/unary-expr (lp/emit-expr independent-relation args)
    (fn [independent-col-types]
      (let [{:keys [param-types] :as dependent-args} (-> args
                                                         (update :param-types
                                                                 (fnil into {})
                                                                 (map (fn [[ik dk]]
                                                                        (if-let [col-type (get independent-col-types ik)]
                                                                          [dk col-type]
                                                                          (throw
                                                                            (err/illegal-arg
                                                                              :core2.apply/missing-column
                                                                              {::err/message (str "Column missing from independent relation: " ik)
                                                                               :column ik})))))
                                                                 columns))
            {dependent-col-types :col-types, ->dependent-cursor :->cursor} (lp/emit-expr dependent-relation dependent-args)
            out-dependent-col-types (zmatch mode
                                      [:mark-join mark-spec]
                                      (let [[col-name _expr] (first (:mark-join mark-spec))]
                                        {col-name [:union #{:null :bool}]})

                                      [:otherwise simple-mode]
                                      (case simple-mode
                                        :cross-join dependent-col-types

                                        (:left-outer-join :single-join)
                                        (-> dependent-col-types types/with-nullable-cols)

                                        (:semi-join :anti-join) {}))]

        {:col-types (merge-with types/merge-col-types independent-col-types out-dependent-col-types)

         :->cursor (let [mode-strat (->mode-strategy mode out-dependent-col-types)

                         open-dependent-cursor
                         (zmatch mode
                           [:mark-join mark-spec]
                           (let [[_col-name expr] (first (:mark-join mark-spec))
                                 projection-spec (expr/->expression-projection-spec "_expr" expr
                                                                                    {:col-types dependent-col-types
                                                                                     :param-types param-types})]
                             (fn [{:keys [allocator params] :as query-opts}]
                               (let [^ICursor dep-cursor (->dependent-cursor query-opts)]
                                 (reify ICursor
                                   (tryAdvance [_ c]
                                     (.tryAdvance dep-cursor (reify Consumer
                                                               (accept [_ in-rel]
                                                                 (with-open [match-vec (.project projection-spec allocator in-rel params)]
                                                                   (.accept c (iv/->indirect-rel [match-vec])))))))

                                   (close [_] (.close dep-cursor))))))

                           [:otherwise _] ->dependent-cursor)]

                     (fn [{:keys [allocator] :as query-opts} independent-cursor]
                       (ApplyCursor. allocator mode-strat independent-cursor
                                     (reify IDependentCursorFactory
                                       (openDependentCursor [_this in-rel idx]
                                         (open-dependent-cursor (-> query-opts
                                                                    (update :params
                                                                            (fn [params]
                                                                              (iv/->indirect-rel (concat params
                                                                                                         (for [[ik dk] columns]
                                                                                                           (-> (.vectorForName in-rel (name ik))
                                                                                                               (.select (int-array [idx]))
                                                                                                               (.withName (name dk)))))
                                                                                                 1))))))))))}))))
