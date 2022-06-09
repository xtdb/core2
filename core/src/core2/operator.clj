(ns core2.operator
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.logical-plan :as lp]
            core2.operator.apply
            core2.operator.arrow
            core2.operator.csv
            core2.operator.group-by
            core2.operator.join
            core2.operator.max-1-row
            core2.operator.order-by
            core2.operator.project
            core2.operator.rename
            [core2.operator.scan :as scan]
            core2.operator.select
            core2.operator.set
            [core2.operator.table :as table]
            core2.operator.top
            core2.operator.unwind
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import (core2 ICursor IResultCursor IResultSet)
           java.time.Clock
           (java.util Iterator)
           (java.util.function Consumer)
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

(defn- args->srcs+params [args]
  (if-not (map? args)
    (recur {'$ args})
    (-> (group-by #(if (lp/source-sym? (key %)) :srcs :params) args)
        (update-vals #(into {} %)))))

(defn- ->scan-col-types [q srcs]
  (let [scan-exprs (->> (lp/child-exprs q) (filterv (comp #{:scan} :op)))]
    (scan/->scan-col-types scan-exprs srcs)))

(deftype ResultCursor [^BufferAllocator allocator, ^ICursor cursor, ^Clock clock, col-types]
  ;; we could probably move this closer into the expression engine,
  ;; but it needs some consideration around the EE's caching behaviour.
  IResultCursor
  (columnTypes [_] col-types)
  (tryAdvance [_ c]
    (binding [expr/*clock* clock]
      (.tryAdvance cursor c)))

  (characteristics [_] (.characteristics cursor))
  (estimateSize [_] (.estimateSize cursor))
  (getComparator [_] (.getComparator cursor))
  (getExactSizeIfKnown [_] (.getExactSizeIfKnown cursor))
  (hasCharacteristics [_ c] (.hasCharacteristics cursor c))
  (trySplit [_] (.trySplit cursor))

  (close [_]
    (.close cursor)
    (.close allocator)))

(defn open-ra
  (^core2.IResultCursor [query] (open-ra query {}))
  (^core2.IResultCursor [query args] (open-ra query args {}))

  (^core2.IResultCursor [query args {:keys [default-valid-time default-tz] :as query-opts}]
   (let [conformed-query (s/conform ::lp/logical-plan query)]
     (when (s/invalid? conformed-query)
       (throw (err/illegal-arg :malformed-query
                               {:plan query
                                :args args
                                :explain (s/explain-data ::lp/logical-plan query)})))

     (let [allocator (RootAllocator.)]
       (try
         (let [default-valid-time (or default-valid-time (.instant expr/*clock*))
               ;; will later be provided as part of the 'SQL session' (see §6.32)
               default-tz (or default-tz (.getZone expr/*clock*))
               clock (Clock/fixed default-valid-time default-tz)]

           (binding [expr/*clock* clock]
             (let [{:keys [srcs params]} (args->srcs+params args)
                   scan-col-types (->scan-col-types conformed-query srcs)

                   ;; now that we're taking scan-col-types out, we might be able to cache emit-expr
                   {:keys [col-types ->cursor]} (lp/emit-expr conformed-query
                                                              {:src-keys (set (keys srcs)),
                                                               :table-keys (->> (for [[src-k src-v] srcs
                                                                                      :when (sequential? src-v)]
                                                                                  [src-k (table/table->keys src-v)])
                                                                                (into {}))
                                                               :scan-col-types scan-col-types
                                                               :param-names (set (keys params))})
                   cursor (->cursor (into query-opts
                                          {:srcs srcs, :params params
                                           :default-valid-time default-valid-time
                                           :allocator allocator}))]

               (ResultCursor. allocator cursor clock col-types))))
         (catch Throwable t
           (util/try-close allocator)
           (throw t)))))))

(deftype CursorResultSet [^IResultCursor cursor
                          ^:unsynchronized-mutable ^Iterator next-values]
  IResultSet
  (columnTypes [_] (.columnTypes cursor))

  (hasNext [res]
    (boolean
     (or (and next-values (.hasNext next-values))
         ;; need to call rel->rows eagerly - the rel may have been reused/closed after
         ;; the tryAdvance returns.
         (and (.tryAdvance cursor
                           (reify Consumer
                             (accept [_ rel]
                               (set! (.-next-values res)
                                     (.iterator (iv/rel->rows rel))))))
              next-values
              (.hasNext next-values)))))

  (next [_] (.next next-values))
  (close [_] (.close cursor)))

(defn cursor->result-set ^core2.IResultSet [^ICursor cursor]
  (CursorResultSet. cursor nil))

(defn query-ra
  ([query inputs] (query-ra query inputs {}))
  ([query inputs query-opts]
   (with-open [res (cursor->result-set (open-ra query inputs query-opts))]
     (vec (iterator-seq res)))))
