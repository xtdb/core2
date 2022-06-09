(ns core2.operator.table
  (:require [clojure.core.match :refer [match]]
            [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.logical-plan :as lp]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2 ICursor)
           (core2.vector IIndirectRelation)
           (java.util ArrayList HashSet HashMap Set)
           java.util.function.Function
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.complex DenseUnionVector)))

(defmethod lp/ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::lp/column :kind set?))
         :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                      :param ::lp/param)))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator allocator
                      ^long row-count
                      cols
                      ^:unsynchronized-mutable done?]
  ICursor
  (tryAdvance [this c]
    (if (or done? (nil? cols))
      false
      (do
        (set! (.done? this) true)

        (let [out-cols (ArrayList. (count cols))]
          (try
            (doseq [[k vs] cols]
              (let [out-vec (DenseUnionVector/empty (name k) allocator)
                    out-writer (.asDenseUnion (vw/vec->writer out-vec))]
                (.add out-cols (iv/->direct-vec out-vec))
                (dorun
                 (map-indexed (fn [idx v]
                                (util/set-value-count out-vec idx)

                                (.startValue out-writer)
                                (doto (.writerForType out-writer (types/value->col-type v))
                                  (.startValue)
                                  (->> (types/write-value! v))
                                  (.endValue))
                                (.endValue out-writer))

                              vs))))

            (catch Exception e
              (run! util/try-close out-cols)
              (throw e)))

          (with-open [^IIndirectRelation out-rel (iv/->indirect-rel out-cols row-count)]
            (.accept c out-rel)
            true)))))

  (close [_]))

(defn- restrict-cols [col-types {:keys [explicit-col-names]}]
  (cond-> col-types
    explicit-col-names (-> (->> (merge (zipmap explicit-col-names (repeat :null))))
                           (select-keys explicit-col-names))))

(defn- emit-rows-table [rows table-expr {:keys [param-types] :as opts}]
  (let [col-type-sets (HashMap.)
        row-count (count rows)
        out-rows (ArrayList. row-count)]
    (doseq [row rows]
      (let [out-row (HashMap.)]
        (doseq [[k v] row
                :let [k (symbol k)]]
          (let [expr (expr/form->expr v opts)
                ^Set col-type-set (.computeIfAbsent col-type-sets k (reify Function (apply [_ _] (HashSet.))))]
            (case (:op expr)
              :literal (do
                         (.add col-type-set (types/value->col-type v))
                         (.put out-row k v))

              :param (let [{:keys [param]} expr]
                       (.add col-type-set (get param-types param))
                       (.put out-row k (fn [{:keys [params]}]
                                         (get params param))))

              ;; HACK: this is quite heavyweight to calculate a single value -
              ;; the EE doesn't yet have an efficient means to do so...
              (let [projection-spec (expr/->expression-projection-spec "_scalar" v opts)]
                (.add col-type-set (.getColumnType projection-spec))
                (.put out-row k (fn [{:keys [allocator params]}]
                                  (with-open [out-vec (.project projection-spec allocator (iv/->indirect-rel [] 1) params)]
                                    (types/get-object (.getVector out-vec) (.getIndex out-vec 0)))))))))
        (.add out-rows out-row)))

    (let [col-types (-> col-type-sets
                        (->> (into {} (map (juxt key (comp #(apply types/merge-col-types %) val)))))
                        (restrict-cols table-expr))]
      {:col-types col-types
       :->table (fn [opts]
                  {:row-count row-count
                   :cols (when (pos? row-count)
                           (->> (for [col-name (keys col-types)]
                                  [col-name (vec (for [row out-rows
                                                       :let [v (get row col-name)]]
                                                   (if (fn? v) (v opts) v)))])
                                (into {})))})})))

(defn- param-type->col-types [param-type]
  (letfn [(->struct-cols-inner [col-type]
            (match col-type
              [:struct struct-cols] #{struct-cols}
              :else nil))

          (->struct-cols-outer [col-type]
            (match col-type
              [:list [:union inner-types]] (->> inner-types (into #{} (mapcat ->struct-cols-inner)))
              [:list inner-type] (->struct-cols-inner inner-type)
              [:fixed-size-list _ [:union inner-types]] (->> inner-types (into #{} (mapcat ->struct-cols-inner)))
              [:fixed-size-list _ inner-type] (->struct-cols-inner inner-type)
              [:union inner-types] (->> inner-types (into #{} (mapcat ->struct-cols-outer)))
              :else nil))]

    (let [struct-cols (->struct-cols-outer param-type)]
      (->> (for [table-key (into #{} (mapcat keys) struct-cols)]
             [(symbol table-key) (->> (into #{} (map #(get % table-key :null)) struct-cols)
                                      (apply types/merge-col-types))])
           (into {})))))

(defn- emit-param-table [param table-expr {:keys [param-types]}]
  (let [col-types (-> (or (get param-types param)
                          (throw (err/illegal-arg :unknown-table
                                                  {::err/message "Table refers to unknown param"
                                                   :param param, :params (set (keys param-types))})))
                      (param-type->col-types)
                      (restrict-cols table-expr))]

    {:col-types col-types
     :->table (fn [{:keys [params]}]
                (let [rows (get params param)]
                  {:row-count (count rows)
                   :cols (when (seq rows)
                           (->> (for [col-name (keys col-types)
                                      :let [col-k (keyword col-name)
                                            col-s (symbol col-name)]]
                                  [col-name (vec (for [row rows]
                                                   (get row col-k (get row col-s))))])
                                (into {})))}))}))

(defmethod lp/emit-expr :table [{:keys [table] :as table-expr} opts]
  (let [{:keys [col-types ->table]} (match table
                                      [:rows rows] (emit-rows-table rows table-expr opts)
                                      [:param param] (emit-param-table param table-expr opts))]

    {:col-types col-types
     :->cursor (fn [{:keys [allocator] :as opts}]
                 (let [{:keys [^long row-count cols]} (->table opts)]
                   (TableCursor. allocator row-count cols false)))}))
