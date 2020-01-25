(ns crux.wcoj
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [crux.datalog :as cd])
  (:import [clojure.lang IPersistentCollection IPersistentMap Symbol Keyword]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector ElementAddressableVector BigIntVector BitVector Float8Vector
            TinyIntVector ValueVector VarBinaryVector VarCharVector]
           org.apache.arrow.vector.complex.StructVector
           org.apache.arrow.vector.types.pojo.FieldType
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.vector.util.Text
           org.apache.arrow.memory.util.ArrowBufPointer
           java.util.Arrays))

(set! *unchecked-math* :warn-on-boxed)
(s/check-asserts true)

;; generic-join

(defprotocol Relation
  (table-scan [this db])
  (table-filter [this db var-bindings])
  (insert [this value])
  (delete [this value]))

(defprotocol Db
  (assertion [this relation-name value])
  (retraction [this relation-name value])
  (ensure-relation [this relation-name relation-factory])
  (relation-by-name [this relation-name]))

(defprotocol Unification
  (unify [this that]))

(defn- execute-constraints [constraints value]
  (reduce
   (fn [value [op arg]]
     (if-let [diff (if (instance? ArrowBufPointer value)
                     (.compareTo ^ArrowBufPointer value arg)
                     (compare value arg))]
       (if (case op
             < (neg? diff)
             <= (not (pos? diff))
             > (pos? diff)
             >= (nat-int? diff)
             = (zero? diff)
             != (not (zero? diff)))
         value
         (reduced nil))))
   value
   constraints))

(defn- constrained [this that]
  (if-let [constraints (::constraints (meta this))]
    (if (cd/logic-var? that)
      (let [that (vary-meta that update ::constraints into constraints)]
        [that that])
      (when (execute-constraints constraints that)
        [that that]))
    [that that]))

(extend-protocol Unification
  (class (byte-array 0))
  (unify [this that]
    (cond
      (cd/logic-var? that)
      (unify that this)

      (and (bytes? that)
           (Arrays/equals ^bytes this ^bytes that))
      [this that]))

  Symbol
  (unify [this that]
    (cond
      (= this that)
      [this this]

      (cd/logic-var? that)
      (constrained that this)

      (cd/logic-var? this)
      (constrained this that)))

  IPersistentCollection
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (and (coll? that)
                 (= (count this) (count that)))
        (first
         (reduce
          (fn [[acc smap] [x y]]
            (if-let [[xu yu] (seq (unify (get smap x x) (get smap y y)))]
              [(conj acc [xu yu])
               (cond-> smap
                 (not= '_ x) (assoc x xu)
                 (not= '_ y) (assoc y yu))]
              (reduced nil)))
          [[] {}]
          (mapv vector this that))))))

  Object
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (= this that)
        [this this])))

  nil
  (unify [this that]
    (if (cd/logic-var? that)
      (unify that this)
      (when (nil? that)
        [this this]))))

(defn- find-vars [body]
  (let [vars (atom [])]
    (w/postwalk (fn [x]
                  (when (cd/logic-var? x)
                    (swap! vars conj x))
                  x)
                body)
    (set @vars)))

(defn- ensure-unique-logic-var [var]
  (if (cd/logic-var? var)
    (gensym var)
    var))

(defn- contains-duplicate-vars? [var-bindings]
  (let [vars (filter cd/logic-var? var-bindings)]
    (not= (distinct vars) vars)))

(def ^:private literal-hierarchy
  (-> (make-hierarchy)
      (derive :equality-predicate :predicate)
      (atom)))

(declare term->value)

(defmulti ^:private new-bound-vars
  (fn [known-vars extra-logical-vars [type]]
    type)
  :hierarchy literal-hierarchy)

(defmethod new-bound-vars :predicate [known-vars extra-logical-vars [_ literal]]
  (let [vars (find-vars literal)]
    (when (set/superset? known-vars (set/intersection vars extra-logical-vars))
      vars)))

(defmethod new-bound-vars :not-predicate [known-vars _ [_ literal]]
  (let [vars (find-vars literal)]
    (when (set/superset? known-vars vars)
      #{})))

(defmethod new-bound-vars :external-query [known-vars _ [_ {:keys [terms variable]}]]
  (when (set/superset? known-vars (find-vars terms))
    #{variable}))

(defn- constraint-predicate? [equality-predicate]
  (= 1 (count (find-vars equality-predicate))))

(defn- new-constraint [op term value lhs?]
  [(if-not lhs?
     (get '{< >=
            <= >
            > <=
            >= <} op op)
     op)
   value])

(defn- equality-predicate-to-constraint [[_ {:keys [lhs op rhs]}]]
  (let [lhs (term->value lhs)
        rhs (term->value rhs)]
    (if (cd/logic-var? lhs)
      {lhs [(new-constraint op lhs rhs true)]}
      {rhs [(new-constraint op rhs lhs false)]})))

(defn- reorder-body [head body]
  (let [{:keys [external-query equality-predicate] :as literals} (group-by first body)
        extra-logical-vars (set (for [[_ {:keys [variable]}] external-query]
                                  variable))
        constraint-predicates (filter constraint-predicate? equality-predicate)]
    (loop [[[type :as literal] & new-body :as body] (remove (set constraint-predicates) body)
           acc []
           known-vars (set/difference (find-vars head) extra-logical-vars)
           circular-check 0]
      (if literal
        (if-let  [new-vars (new-bound-vars known-vars extra-logical-vars literal)]
          (recur new-body (conj acc literal) (set/union known-vars new-vars) 0)
          (if (= (count body) circular-check)
            (throw (IllegalArgumentException. "Circular dependency."))
            (recur (conj (vec new-body) literal) acc known-vars (inc circular-check))))
        acc))))

(defn- min-aggregate [x y]
  ((fnil min x x) x y))

(defn- max-aggregate [x y]
  ((fnil max x x) x y))

(defn- count-aggregate [x y]
  ((fnil inc 0) y))

(defn- sum-aggregate [x y]
  ((fnil + x 0) x y))

(defn- build-aggregates [{:keys [terms] :as head}]
  (when-let [aggregate-ops (seq (for [[idx [type term]] (map-indexed vector terms)
                                      :when (= :aggregate type)]
                                  [idx (get {'min crux.wcoj/min-aggregate
                                             'max crux.wcoj/max-aggregate
                                             'count crux.wcoj/count-aggregate
                                             'sum crux.wcoj/sum-aggregate}
                                            (:op term))]))]
    {:group-idxs (vec (for [[idx [type term]] (map-indexed vector terms)
                            :when (not= :aggregate type)]
                        idx))
     :aggregate-ops aggregate-ops}))

(defn- enrich-with-constraints [{:keys [body] :as rule}]
  (let [{:keys [equality-predicate] :as literals} (group-by first body)
        constraint-smap (->> (for [[var constraints] (->> (filter constraint-predicate? equality-predicate)
                                                          (map equality-predicate-to-constraint)
                                                          (apply merge-with concat))]
                               [var (vary-meta var assoc ::constraints constraints)])
                             (into {}))]
    (w/postwalk-replace constraint-smap rule)))

(defn- rule->query-plan [rule]
  (let [{:keys [head body] :as conformed-rule} (s/conform :crux.datalog/rule rule)
        _ (assert (set/superset? (find-vars body) (find-vars head))
                  "rule does not satisfy safety requirement for head variables")
        {:keys [head body]} (enrich-with-constraints conformed-rule)
        head-vars (find-vars head)
        body (reorder-body head body)
        body-vars (find-vars body)]
    {:existential-vars (set/difference body-vars head-vars)
     :aggregates (build-aggregates head)
     :rule rule
     :head head
     :body body}))

(defmulti ^:private term->binding
  (fn [[type term]]
    type))

(defmethod term->binding :constant [[_ term]]
  (gensym 'constant))

(defmethod term->binding :aggregate [[_ term]]
  (first (:variable term)))

(defmethod term->binding :default [[_ term]]
  term)

(defn- quote-term [x]
  (list 'quote x))

(defmulti ^:private term->value
  (fn [[type term]]
    type))

(defmethod term->value :constant [[_ term]]
  (if (symbol? term)
    (quote-term term)
    term))

(defmethod term->value :aggregate [[_ term]]
  (first (:variable term)))

(defmethod term->value :default [[_ term]]
  term)

(defmulti ^:private term->signature
  (fn [[type term]]
    type))

(defmethod term->signature :aggregate [[_ term]]
  (first (:variable term)))

(defmethod term->signature :default [[_ term]]
  term)

(defn- predicate->clojure [{:keys [db-sym]} {:keys [symbol terms]}]
  `(crux.wcoj/table-filter
    (crux.wcoj/relation-by-name ~db-sym '~symbol)
    ~db-sym ~(mapv term->value terms)))

(defn- unification->clojure [bindings x y]
  `[:let [[~@bindings :as unified?#] (crux.wcoj/unify ~x ~y)]
    :when unified?#])

(defmulti ^:private datalog->clojure (fn [query-plan [type]]
                                       type))

(defmethod datalog->clojure :predicate [query-plan [_ {:keys [terms] :as predicate}]]
  (let [term-vars (mapv term->binding terms)]
    [term-vars (predicate->clojure query-plan predicate)]))

(defmethod datalog->clojure :equality-predicate [_ [_ {:keys [lhs op rhs]}]]
  (let [op-fn (get '{!= (complement crux.wcoj/unify)} op op)]
    (if (= '= op)
      (unification->clojure
       [(term->binding lhs) (term->binding rhs)]
       (term->value lhs) (term->value rhs))
      `[:when (~op-fn ~(term->value lhs) ~(term->value rhs))])))

(defmethod datalog->clojure :not-predicate [query-plan [_ {:keys [predicate]}]]
  `[:when (empty? ~(predicate->clojure query-plan predicate))])

(defmethod datalog->clojure :external-query [_ [_ {:keys [variable symbol terms]}]]
  (unification->clojure [variable] variable `(~symbol ~@(mapv term->value terms))))

(defn aggregate [{:keys [group-idxs aggregate-ops]} tuples]
  (vals
   (reduce
    (fn [acc tuple]
      (update acc (mapv tuple group-idxs)
              (fn [group-acc]
                (reduce
                 (fn [acc [idx op]]
                   (update acc idx op (get group-acc idx)))
                 tuple aggregate-ops))))
    {}
    tuples)))

(defn- query-plan->clojure [{:keys [existential-vars aggregates head body] :as query-plan}]
  (let [{:keys [symbol terms]} head
        db-sym (gensym 'db)
        args-sym (gensym 'args)
        query-plan (assoc query-plan :db-sym db-sym)
        bindings (mapcat (partial datalog->clojure query-plan) body)
        arg-vars (mapv term->binding terms)
        args-signature (mapv (comp quote-term term->signature) terms)]
    `(fn ~symbol
       ([~db-sym] (~symbol ~db-sym '~(vec (repeat (count arg-vars) '_))))
       ([~db-sym ~args-sym]
        (->> (for [loop# [nil]
                   ~@(unification->clojure (map vector arg-vars) args-sym args-signature)
                   :let [~@(interleave existential-vars (map quote-term existential-vars))]
                   ~@bindings]
               ~arg-vars)
             ~(if aggregates
                `(crux.wcoj/aggregate '~aggregates)
                `(identity)))))))

(defn- compile-rule-no-memo [rule]
  (let [query-plan (rule->query-plan rule)
        clojure-source (query-plan->clojure query-plan)]
    (with-meta
      (eval clojure-source)
      (assoc query-plan :source clojure-source))))

(def ^:private compile-rule (memoize compile-rule-no-memo))

(defn- execute-rules-no-memo [rules db var-bindings]
  (->> (for [rule rules]
         (if (empty? var-bindings)
           ((compile-rule rule) db)
           ((compile-rule rule) db var-bindings)))
       (apply concat)
       (distinct)))

(defn- rule-memo-key [rules var-bindings]
  [(System/identityHashCode rules)
   (-> (zipmap (filter cd/logic-var? var-bindings)
               (for [id (range)]
                 (symbol (str "crux.wcoj/variable_" id))))
       (replace var-bindings))])

(defn- execute-rules-memo [rules db var-bindings]
  (let [db (vary-meta db update :rule-memo-state #(or % (atom {})))
        {:keys [rule-memo-state]} (meta db)
        memo-key (rule-memo-key rules var-bindings)
        memo-value (get @rule-memo-state memo-key ::not-found)]
    (if (= ::not-found memo-value)
      (doto (execute-rules-no-memo rules db var-bindings)
        (->> (swap! rule-memo-state assoc memo-key)))
      memo-value)))

(defn- execute-rules [rules db var-bindings]
  (cond->> (execute-rules-memo rules db (mapv ensure-unique-logic-var var-bindings))
    (contains-duplicate-vars? var-bindings) (filter #(unify var-bindings %))))

(defrecord RuleRelation [name rules]
  Relation
  (table-scan [this db]
    (table-filter this db nil))

  (table-filter [this db var-bindings]
    (execute-rules rules db var-bindings))

  (insert [this rule]
    (s/assert :crux.datalog/rule rule)
    (update this :rules conj rule))

  (delete [this rule]
    (update this :rules disj rule)))

(defn- new-rule-relation [name]
  (->RuleRelation name #{}))

(extend-protocol Relation
  nil
  (table-scan [this db])

  (table-filter [this db var-bindings])

  (insert [this tuple]
    (throw (UnsupportedOperationException.)))

  (delete [this tuple]
    (throw (UnsupportedOperationException.)))

  IPersistentCollection
  (table-scan [this db]
    (seq this))

  (table-filter [this db var-bindings]
    (for [tuple (table-scan this db)
          :when (unify tuple var-bindings)]
      tuple))

  (insert [this tuple]
    (conj this tuple))

  (delete [this tuple]
    (disj this tuple)))

(defn new-sorted-set-relation [relation-name]
  (with-meta (sorted-set) {:name relation-name}))

(defrecord CombinedRelation [name rules tuples]
  Relation
  (table-scan [this db]
    (concat (table-scan tuples db)
            (table-scan rules db)))

  (table-filter [this db var-bindings]
    (concat (table-filter tuples db var-bindings)
            (table-filter rules db var-bindings)))

  (insert [this value]
    (if (s/valid? :crux.datalog/rule value)
      (update this :rules insert value)
      (update this :tuples insert value)))

  (delete [this value]
    (if (s/valid? :crux.datalog/rule value)
      (update this :rules delete value)
      (update this :tuples delete value))))

(def ^:dynamic *tuple-relation-factory* new-sorted-set-relation)

(defn- new-combined-relation [relation-name]
  (->CombinedRelation
   relation-name
   (new-rule-relation relation-name)
   (*tuple-relation-factory* relation-name)))

(def ^:dynamic *relation-factory* new-combined-relation)

(extend-type IPersistentMap
  Db
  (assertion [this relation-name value]
    (update (ensure-relation this relation-name *relation-factory*)
            relation-name
            insert
            value))

  (retraction [this relation-name value]
    (update this
            relation-name
            delete
            value))

  (ensure-relation [this relation-name relation-factory]
    (cond-> this
      (not (contains? this relation-name)) (assoc relation-name (relation-factory relation-name))))

  (relation-by-name [this relation-name]
    (get this relation-name)))

(defn query-by-name
  ([db rule-name]
   (table-scan (relation-by-name db rule-name) db))
  ([db rule-name args]
   (table-filter (relation-by-name db rule-name) db args)))

(defn- query-conformed-datalog [db {{:keys [symbol terms]} :head}]
  (let [args (mapv second terms)]
    (query-by-name db symbol args)))

(defn query [db query]
  (s/assert :crux.datalog/query query)
  (query-conformed-datalog db (s/conform :crux.datalog/query query)))

(defn assert-all [db relation-name tuples]
  (reduce (fn [db tuple]
            (assertion db relation-name tuple)) db tuples))

(defn tuple->datalog-str [relation-name tuple]
  (str relation-name
       (when (seq tuple)
         (str "(" (str/join ", " tuple) ")"))
       "."))

(def ^:private execution-hierarchy
  (-> (make-hierarchy)
      (derive :assertion :modification)
      (derive :retraction :modification)
      (atom)))

(defmulti ^:private execute-statement
  (fn [db [type]]
    type)
  :hierarchy execution-hierarchy)

(defmethod execute-statement :query [db [type {{:keys [symbol]} :head :as statement}]]
  (doseq [tuple (sort (distinct (query-conformed-datalog db statement)))]
    (println (tuple->datalog-str symbol tuple)))
  db)

(defmethod execute-statement :requirement [db [type {:keys [identifier]}]]
  (require (first identifier))
  db)

(defmethod execute-statement :modification [db [type {:keys [clause]}]]
  (let [op (get {:assertion assertion :retraction retraction} type)
        [type clause] clause
        {:keys [symbol terms]} (:head clause)]
    (case type
      :fact
      (op db symbol (mapv second terms))

      :rule
      (op db symbol (vec (s/unform :crux.datalog/rule clause))))))

(defn execute
  ([datalog]
   (execute {} datalog))
  ([db datalog]
   (s/assert :crux.datalog/program datalog)
   (->> (s/conform :crux.datalog/program datalog)
        (reduce execute-statement db))))

(defn -main [& [f :as args]]
  (execute (cd/parse-datalog (io/reader (or f *in*)))))

;; Arrow

(def ^:private ^BufferAllocator
  allocator (RootAllocator. Long/MAX_VALUE))

(defn- init-struct [^StructVector relation column-template]
  (reduce
   (fn [^StructVector relation [idx column-template]]
     (let [column-template (if-let [[[_ value]] (and (cd/logic-var? column-template)
                                                     (::constraints (meta column-template)))]
                             value
                             column-template)
           column-type (.getSimpleName (class column-template))
           [^FieldType field-type ^Class vector-class]
           (case (symbol column-type)
             (Integer Long) [(FieldType/nullable (.getType Types$MinorType/BIGINT))
                             BigIntVector]
             (Float Double) [(FieldType/nullable (.getType Types$MinorType/FLOAT8))
                             Float8Vector]
             String [(FieldType/nullable (.getType Types$MinorType/VARCHAR))
                     VarCharVector]
             Boolean [(FieldType/nullable (.getType Types$MinorType/TINYINT))
                      TinyIntVector]
             [(FieldType/nullable (.getType Types$MinorType/VARBINARY))
              VarBinaryVector])]
       (doto relation
         (.addOrGet
          (str idx "_" (str/lower-case column-type))
          field-type
          vector-class))))
   relation
   (map-indexed vector column-template)))

(defn- arrow->clojure [value]
  (cond
    (instance? Text value)
    (str value)

    (bytes? value)
    (edn/read-string (String. ^bytes value "UTF-8"))

    (int? value)
    (bit-xor (Long/reverseBytes (long value)) Long/MIN_VALUE)

    (float? value)
    (let [x (Double/doubleToLongBits value)]
      (Double/longBitsToDouble (bit-xor x (bit-or Long/MIN_VALUE (bit-shift-left x (dec Long/SIZE))))))

    (instance? Byte value)
    (= 1 value)

    :else
    value))

(defn- arrow->tuple [^StructVector struct ^long idx]
  (loop [n 0
         acc []]
    (if (= n (.size struct))
      acc
      (let [column (.getChildByOrdinal struct n)
            value (.getObject column idx)]
        (recur (inc n)
               (conj acc (arrow->clojure value)))))))

(defn- clojure->arrow [value]
  (cond
    (string? value)
    (Text. (.getBytes ^String value "UTF-8"))

    (boolean? value)
    (if value 1 0)

    (int? value)
    (Long/reverseBytes (bit-xor (long value) Long/MIN_VALUE))

    (float? value)
    (let [l (Double/doubleToLongBits value)]
      (Double/longBitsToDouble (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE))))

    :else
    (.getBytes (pr-str value) "UTF-8")))

(defn- insert-clojure-value-into-column [^ValueVector column ^long idx v]
  (if-let [[[_ value]] (and (cd/logic-var? v)
                            (::constraints (meta v)))]
    (insert-clojure-value-into-column column idx value)
    (cond
      (instance? BigIntVector column)
      (.setSafe ^BigIntVector column idx ^long (clojure->arrow v))

      (instance? Float8Vector column)
      (.setSafe ^Float8Vector column idx ^double (clojure->arrow v))

      (instance? VarCharVector column)
      (.setSafe ^VarCharVector column idx ^Text (clojure->arrow v))

      (instance? TinyIntVector column)
      (.setSafe ^TinyIntVector column idx ^long (clojure->arrow v))

      (instance? VarBinaryVector column)
      (.setSafe ^VarBinaryVector column idx ^bytes (clojure->arrow v))

      :else
      (throw (IllegalArgumentException.)))))

(def ^:private ^{:tag 'long} default-vector-size 128)

(defn- unifiers->arrow [unifier-vector var-bindings]
  (vec (for [[^ElementAddressableVector unify-column var] (map vector unifier-vector var-bindings)]
         (if (cd/logic-var? var)
           (vary-meta var update
                      ::constraints
                      (fn [constraints]
                        (reduce
                         (fn [acc [^long idx [op value]]]
                           (insert-clojure-value-into-column unify-column (inc idx) value)
                           (conj acc [op (.getDataPointer unify-column (inc idx))]))
                         [] (map-indexed vector constraints))))
           (.getDataPointer ^ElementAddressableVector unify-column 0)))))

(defn- arrow-seq [^StructVector struct var-bindings]
  (let [vector-size (min default-vector-size (.getValueCount struct))
        init-selection-vector (doto (BitVector. "" allocator)
                                (.setValueCount vector-size)
                                (.setInitialCapacity vector-size))
        unify-tuple? (contains-duplicate-vars? var-bindings)
        unifier-vector (insert
                        (StructVector/empty nil allocator)
                        var-bindings)
        unifiers (unifiers->arrow unifier-vector var-bindings)
        pointer (ArrowBufPointer.)]
    (->> (for [start-idx (range 0 (.getValueCount struct) vector-size)
               :let [start-idx (long start-idx)
                     limit (min (.getValueCount struct) (+ start-idx vector-size))]]
           (loop [n 0
                  ^BitVector selection-vector nil]
             (if (and (< n (.size struct)) var-bindings)
               (if-let [unifier (get unifiers n)]
                 (let [column ^ElementAddressableVector (.getChildByOrdinal struct n)]
                   (recur (inc n)
                          (loop [idx start-idx
                                 selection-vector (or selection-vector init-selection-vector)
                                 selection-offset 0]
                            (if (< idx limit)
                              (recur (inc idx)
                                     (cond
                                       (.isNull struct idx)
                                       (doto selection-vector
                                         (.setSafe selection-offset 0))

                                       (and (pos? n)
                                            (zero? (.get selection-vector selection-offset)))
                                       selection-vector

                                       :else
                                       (doto selection-vector
                                         (.setSafe selection-offset (if (unify unifier (.getDataPointer column idx pointer))
                                                                      1
                                                                      0))))
                                     (inc selection-offset))
                              selection-vector))))
                 (recur (inc n) selection-vector))
               (if selection-vector
                 (loop [selection-offset 0
                        acc []]
                   (if (< selection-offset vector-size)
                     (recur (inc selection-offset)
                            (if (zero? (.get selection-vector selection-offset))
                              acc
                              (let [tuple (arrow->tuple struct (+ start-idx selection-offset))]
                                (if (or (not unify-tuple?)
                                        (unify tuple var-bindings))
                                  (conj acc tuple)
                                  acc))))
                     acc))
                 (loop [idx start-idx
                        acc []]
                   (if (< idx limit)
                     (recur (inc idx)
                            (conj acc (arrow->tuple struct idx)))
                     acc))))))
         (apply concat))))

(extend-protocol Relation
  StructVector
  (table-scan [this db]
    (arrow-seq this nil))

  (table-filter [this db var-bindings]
    (arrow-seq this var-bindings))

  (insert [this value]
    (if (and (zero? (.size this)) (pos? (count value)))
      (insert (init-struct this value) value)
      (let [idx (.getValueCount this)]
        (dotimes [n (.size this)]
          (let [v (get value n)
                column (.getChildByOrdinal this n)]
            (insert-clojure-value-into-column column idx v)))

        (doto this
          (.setIndexDefined idx)
          (.setValueCount (inc idx))))))

  (delete [this value]
    (doseq [[idx to-delete] (map-indexed vector (table-scan this {}))
            :when (unify value to-delete)]
      (.setNull this idx))
    this))

(defn new-arrow-struct-relation
  ^org.apache.arrow.vector.complex.StructVector [relation-name]
  (doto (StructVector/empty (str relation-name) allocator)
    (.setInitialCapacity 0)))
