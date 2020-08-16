(ns crux.sql
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.instant :as i]
            [clojure.set :as set]
            [clojure.string :as s]
            [clojure.walk :as w]
            [instaparse.core :as insta])
  (:import [java.util Collection Comparator Date]
           java.util.function.ToDoubleFunction
           java.util.function.Function
           [java.time Duration Period ZoneOffset]
           [java.time.temporal ChronoField TemporalAmount]
           [clojure.lang Associative Counted ILookup IPersistentCollection IPersistentSet IReduceInit MapEntry Seqable]))

(set! *unchecked-math* :warn-on-boxed)

(defn parse-string [x]
  (s/replace (subs x 1 (dec (count x)))
             "''"
             "'"))

(defn parse-date [x]
  (i/read-instant-date (s/replace (parse-string x) " " "T")))

(defn parse-number [x]
  (edn/read-string x))

(defn parse-interval [x [field]]
  (let [x (parse-number (parse-string x))]
    (case field
      :year (Period/ofYears x)
      :month (Period/ofMonths x)
      :day (Period/ofDays x)
      :hour (Duration/ofHours x)
      :minute (Duration/ofMinutes x))))

(defn parse-identifier [x]
  (symbol x))

(defn parse-boolean [[x]]
  (= :true x))

(defn parse-like-pattern [x & [escape]]
  (let [pattern (parse-string x)
        escape (or (some-> escape (parse-string)) "\\")
        regex (if (= "\\" escape)
                (-> pattern
                    (s/replace #"([^\\]|^)(_)" "$1.")
                    (s/replace #"([^\\]|^)(%)" "$1.*")
                    (s/replace "\\_" "_")
                    (s/replace "\\%" "%"))
                (-> pattern
                    (s/replace (re-pattern (str "([^"
                                                escape
                                                "]|^)(_)"))
                               "$1.")
                    (s/replace (re-pattern (str "([^"
                                                escape
                                                "]|^)(%)"))
                               "$1.*")
                    (s/replace (str escape "_") "_")
                    (s/replace (str escape "%") "%")))]
    (re-pattern regex)))

(def parse-sql
  (insta/parser (io/resource "crux/sql.ebnf")
                :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*\\s*' | #'\\s*/[*].*([*]/\\s*|$)'")
                :string-ci true))

(def literal-transform
  {:boolean-literal parse-boolean
   :numeric-literal parse-number
   :unsigned-numeric-literal parse-number
   :date-literal parse-date
   :timestamp-literal parse-date
   :interval-literal parse-interval
   :string-literal parse-string
   :like-pattern parse-like-pattern
   :identifier parse-identifier})

(set! *unchecked-math* false)

(def constant-folding-transform
  {:numeric-minus (fn
                    ([x]
                     (if (number? x)
                       (- x)
                       [:numeric-minus x]))
                    ([x y]
                     (cond
                       (and (instance? Date x)
                            (instance? TemporalAmount y))
                       (Date/from (.toInstant (.minus (.atOffset (.toInstant ^Date x) ZoneOffset/UTC) ^TemporalAmount y)))
                       (and (double? x) (double? y))
                       (double (- (bigdec x) (bigdec y)))
                       (and (number? x) (number? y))
                       (- x y)
                       :else
                       [:numeric-minus x y])))
   :numeric-plus (fn [x y]
                   (cond
                     (and (instance? Date x)
                          (instance? TemporalAmount y))
                     (Date/from (.toInstant (.plus (.atOffset (.toInstant ^Date x) ZoneOffset/UTC) ^TemporalAmount y)))
                     (and (double? x) (double? y))
                     (double (+ (bigdec x) (bigdec y)))
                     (and (number? x) (number? y))
                     (+ x y)
                     :else
                     [:numeric-plus x y]))
   :numeric-multiply (fn [x y]
                       (if (and (number? x) (number? y))
                         (* x y)
                         [:numeric-multiply x y]))
   :numeric-divide (fn [x y]
                     (if (and (number? x) (number? y))
                       (/ x y)
                       [:numeric-divide x y]))
   :numeric-modulo (fn [x y]
                     (if (and (number? x) (number? y))
                       (rem x y)
                       [:numeric-modulo x y]))
   :like-exp (fn
               ([x pattern]
                [:like-exp x pattern])
               ([x not pattern]
                [:boolean-not [:like-exp x pattern]]))
   :in-exp (fn
               ([x y]
                [:in-exp x y])
               ([x not y]
                [:boolean-not [:in-exp x y]]))
   :in-value-list hash-set
   :routine-invocation (fn [f & args]
                         (case f
                           'date (apply i/read-instant-date args)
                           (vec (cons :routine-invocation (cons f args)))))
   :table-exp (fn [x]
                [:select-exp [:select :star] [:from x]])
   :between-exp (fn
                  ([v x y]
                   [:boolean-and
                    [:comp-ge v x]
                    [:comp-le v y]])
                  ([v not x y]
                   [:boolean-or
                    [:comp-lt v x]
                    [:comp-gt v y]]))})

(set! *unchecked-math* :warn-on-boxed)

(def nary-transform
  {:boolean-and (fn [x y]
                  (cond
                    (and (vector? x)
                         (= :boolean-and (first x))
                         (vector? y)
                         (= :boolean-and (first y)))
                    (apply conj x (rest y))
                    (and (vector? x)
                         (= :boolean-and (first x)))
                    (conj x y)
                    :else
                    [:boolean-and x y]))
   :boolean-or (fn [x y]
                 (cond
                   (and (vector? x)
                        (= :boolean-or (first x))
                        (vector? y)
                        (= :boolean-or (first y)))
                   (apply conj x (rest y))
                   (and (vector? x)
                        (= :boolean-or (first x)))
                   (conj x y)
                   :else
                   [:boolean-or x y]))})

(declare symbol-suffix)

(def normalize-transform
  (merge
   {:table-spec (fn [x & [y]]
                  [x y])
    :select-item (fn [x & [y]]
                   [x (or y (if (symbol? x)
                              (symbol-suffix x)
                              (gensym "column_")))])
    :sort-spec (fn [x & [dir]]
                 [x (or dir :asc)])
    :set-function-spec (fn
                         ([type x]
                          [type :all x])
                         ([type quantifier x]
                          [type quantifier x]))}
   (let [constants [:count :sum :avg :min :max :star :all :distinct :asc :desc :year :month :day :hour :minute]]
     (zipmap constants (map constantly constants)))))

(def simplify-transform
  (->> (for [[x y] {:like-exp :like
                    :in-exp :in
                    :case-exp :case
                    :exists-exp :exists
                    :extract-exp :extract
                    :match-exp :match
                    :unique-exp :unique
                    :all-exp :all
                    :any-exp :any
                    :numeric-multiply :*
                    :numeric-divide :/
                    :numeric-plus :+
                    :numeric-minus :-
                    :numeric-modulo :%
                    :boolean-and :and
                    :boolean-or :or
                    :boolean-not :not
                    :comp-eq :=
                    :comp-ne :<>
                    :comp-lt :<
                    :comp-le :<=
                    :comp-gt :>
                    :comp-ge :>=}]
         [x (fn [& args]
              (vec (cons y args)))])
       (into {})))

(defn query->map [[_ & args]]
  (let [select (zipmap (map first args)
                       (map (comp vec rest) args))]
    (reduce
     (fn [acc k]
       (cond-> acc
         (contains? acc k)
         (update k first)))
     select [:where :having :offset :limit])))

(defn map->query [m]
  (vec (cons :select-exp
             (mapv vec
                   (for [[k v] m]
                     (if (contains? #{:where :having :offset :limit} k)
                       [k v]
                       (vec (cons k v))))))))

(defn symbol-suffix [x]
  (symbol (s/replace x #"^.+\." "")))

(defn symbol-prefix [x]
  (symbol (s/replace x #"\..+$" "")))

(defn symbol-with-prefix? [x]
  (boolean (re-find #"\." (str x))))

(defn qualify-transform [x column->tables]
  (w/postwalk
   (fn [x]
     (if (and (vector? x) (= :select-exp (first x)))
       (let [mapping (volatile! {})
             {:keys [from] :as query} (query->map x)
             from (vec (for [[x y] from]
                         (if (vector? x)
                           [x y]
                           [x (or y (let [m (gensym x)]
                                      (vswap! mapping assoc (name x) m)
                                      m))])))]
         (w/postwalk
          (fn [x]
            (if (and (symbol? x) (not (symbol-with-prefix? x)))
              (if-let [ts (get column->tables (name (symbol-suffix x)))]
                (if (contains? @mapping (first ts))
                  (if (= 1 (count ts))
                    (symbol (str (get @mapping (first ts)) "." (name x)))
                    (throw (IllegalArgumentException. (str "Column not unique:" x))))
                  x)
                (symbol x))
              x))
          (map->query (assoc query :from from))))
       x))
   x))

(defn normalize-where [where]
  (cond
    (set? where)
    where
    (= :and (first where))
    (set (rest where))
    (nil? where)
    #{}
    :else
    #{where}))

(defn find-joins [where known-tables]
  (->> (for [x (normalize-where where)
             :when (= := (first x))
             :let [[_ a b] x]
             :when (and (symbol? a)
                        (symbol? b)
                        (not= (symbol-prefix a)
                              (symbol-prefix b))
                        (contains? known-tables (symbol-prefix a))
                        (contains? known-tables (symbol-prefix b)))]
         {:lhs (symbol-prefix a)
          :rhs (symbol-prefix b)
          :using {(symbol-suffix a)
                  (symbol-suffix b)}
          :selection x})))

(defn calculate-join-order [db joins]
  (sort-by (fn [{:keys [lhs rhs]}]
             (max (count (get db (str lhs)))
                  (count (get db (str rhs)))))
           joins))

(defn sub-query? [x]
  (and (vector? x )
       (contains? #{:union :except :intersect :select-exp} (first x))))

(defn find-known-tables [from]
  (set (filter symbol? (map second from))))

(defn find-free-vars [x]
  (let [known-tables (volatile! #{})
        free-vars (volatile! #{})]
    (w/prewalk
     (fn [x]
       (when (and (symbol? x) (nil? (namespace x))
                  (not (contains? @known-tables (symbol-prefix x))))
         (vswap! free-vars conj x))
       (when (and (sub-query? x) (= :select-exp (first x)))
         (vswap! known-tables set/union (find-known-tables (:from (query->map x)))))
       x)
     x)
    @free-vars))

(defn find-base-table->selections [where {:keys [known-vars]}]
  (->> (for [x (normalize-where where)
             :let [vars (remove known-vars (find-free-vars x))]
             :when (and (= 1 (count (distinct (map symbol-prefix vars))))
                        (not-any? sub-query? x))
             :let [[a] (filter symbol? vars)]]
         {(symbol-prefix a) #{x}})
       (apply merge-with set/union)))

(defn extend-scope [x {:keys [known-vars] :as ctx}]
  (let [new-vars (set (remove known-vars (find-free-vars x)))
        ctx (update ctx :known-vars set/union new-vars)]
    [(mapv symbol-suffix new-vars) ctx]))

(defmulti codegen-sql (fn [x _]
                        (if (vector? x)
                          (first x)
                          :default)))

(defn maybe-sub-query [x ctx]
  (codegen-sql x (if (sub-query? x)
                   (assoc ctx :scalar-sub-query? true)
                   ctx)))

(defmethod codegen-sql :not [[_ x] ctx]
  `(not ~(maybe-sub-query x ctx)))

(defmethod codegen-sql :and [[_ & xs] ctx]
  `(and ~@(for [x xs]
            (maybe-sub-query x ctx))))

(defmethod codegen-sql :or [[_ & xs] ctx]
  `(or ~@(for [x xs]
           (maybe-sub-query x ctx))))

(defmethod codegen-sql := [[_ x y] ctx]
  `(= ~(maybe-sub-query x ctx) ~(maybe-sub-query y ctx)))

(defmethod codegen-sql :<> [[_ x y] ctx]
  `(not= ~(maybe-sub-query x ctx) ~(maybe-sub-query y ctx)))

(defn as-double [x]
  (if (number? x)
    x
    (with-meta x {:tag 'double})))

(defn numeric-op [op x y ctx]
  `(~op
    ~(as-double (maybe-sub-query x ctx))
    ~(as-double (maybe-sub-query y ctx))))

(defmethod codegen-sql :< [[_ x y] ctx]
  (if (or (number? x) (number? y))
    (numeric-op '< x y ctx)
    `(neg? (compare ~(maybe-sub-query x ctx) ~(maybe-sub-query y ctx)))))

(defmethod codegen-sql :<= [[_ x y] ctx]
  (if (or (number? x) (number? y))
    (numeric-op '<= x y ctx)
    `(not (pos? (compare ~(maybe-sub-query x ctx) ~(maybe-sub-query y ctx))))))

(defmethod codegen-sql :> [[_ x y] ctx]
  (if (or (number? x) (number? y))
    (numeric-op '> x y ctx)
    `(pos? (compare ~(maybe-sub-query x ctx) ~(maybe-sub-query y ctx)))))

(defmethod codegen-sql :>= [[_ x y] ctx]
  (if (or (number? x) (number? y))
    (numeric-op '>= x y ctx)
    `(not (neg? (compare ~(maybe-sub-query x ctx) ~(maybe-sub-query y ctx))))))

(defmethod codegen-sql :+ [[_ x y] ctx]
  (numeric-op '+ x y ctx))

(defmethod codegen-sql :- [[_ x y] ctx]
  (if y
    (numeric-op '- x y ctx)
    `(- ~(as-double (maybe-sub-query x ctx)))))

(defmethod codegen-sql :* [[_ x y] ctx]
  (numeric-op '* x y ctx))

(defmethod codegen-sql :/ [[_ x y] ctx]
  (numeric-op '/ x y ctx))

(defmethod codegen-sql :% [[_ x y] ctx]
  (numeric-op '% x y ctx))

(defmethod codegen-sql :exists [[_ x] ctx]
  `(boolean (not-empty ~(codegen-sql x (assoc ctx :row-sub-query? true)))))

(defmethod codegen-sql :unique [[_ x] ctx]
  `(apply distinct? ~(codegen-sql x ctx)))

(defmethod codegen-sql :in [[_ x y] ctx]
  (if (sub-query? y)
    `(boolean (some #{~(maybe-sub-query x ctx)} ~(codegen-sql y (assoc ctx :row-sub-query? true))))
    `(contains? ~(codegen-sql y ctx) ~(maybe-sub-query x ctx))))

(defmethod codegen-sql :all [[_ x op y] ctx]
  `(every? #(~@(codegen-sql [op x '%] ctx))
           ~(codegen-sql y (assoc ctx :row-sub-query? true))))

(defmethod codegen-sql :any [[_ x op y] ctx]
  `(boolean (some #(~@(codegen-sql [op x '%] ctx))
                  ~(codegen-sql y (assoc ctx :row-sub-query? true)))))

(defmethod codegen-sql :like [[_ x pattern] ctx]
  `(boolean (re-find ~pattern ~(maybe-sub-query x ctx))))

(defmethod codegen-sql :case [[_ cond then else] ctx]
  `(if ~(maybe-sub-query cond ctx)
     ~(maybe-sub-query then ctx)
     ~(maybe-sub-query else ctx)))

(defmethod codegen-sql :extract [[_ field x] ctx]
  `(.get (.atOffset (.toInstant ~(maybe-sub-query
                                  (if (symbol? x)
                                    (with-meta x {:tag `Date})
                                    x)
                                  ctx)) ZoneOffset/UTC)
         ~(case field
            :year `ChronoField/YEAR
            :month `ChronoField/MONTH_OF_YEAR
            :day `ChronoField/DAY_OF_MONTH
            :hour `ChronoField/HOUR_OF_DAY
            :minute `ChronoField/MINUTE_OF_HOUR)))

(defmethod codegen-sql :routine-invocation [[_ f & args] ctx]
  (case f
    (substr
     substring) (let [[x start length] args]
                  `(let [start# ~(maybe-sub-query start ctx)]
                     (subs ~(maybe-sub-query x ctx)
                           (dec start#)
                           (+ (dec start#) ~(maybe-sub-query length ctx)))))
    `(~f ~@(for [arg args]
             (maybe-sub-query arg ctx)))))

(defn map-to-double [[_ quantifier x] {:keys [group-var] :as ctx}]
  (let [[new-vars ctx] (extend-scope x ctx)]
    (cond->> `(.mapToDouble (.stream ~(with-meta group-var {:tag `Collection}))
                            (reify ToDoubleFunction
                              (applyAsDouble [_# {:strs ~new-vars}]
                                (double ~(maybe-sub-query x ctx)))))
      (= :distinct quantifier) (list '.distinct))))

(defmethod codegen-sql :sum [set-fn ctx]
  `(.sum ~(map-to-double set-fn ctx)))

(defmethod codegen-sql :avg [set-fn ctx]
  `(.getAsDouble (.average ~(map-to-double set-fn ctx))))

(defmethod codegen-sql :min [set-fn ctx]
  `(.getAsDouble (.min ~(map-to-double set-fn ctx))))

(defmethod codegen-sql :max [set-fn ctx]
  `(.getAsDouble (.max ~(map-to-double set-fn ctx))))

(defmethod codegen-sql :count [[_ quantifier x] {:keys [group-var] :as ctx}]
  (let [[new-vars ctx] (extend-scope x ctx)]
    `(count ~(cond->> (if (= :star x)
                        group-var
                        `(map (fn [{:strs ~new-vars}]
                                ~(maybe-sub-query x ctx))
                              ~group-var))
               (= :distinct quantifier) (list 'distinct)))))

(defmethod codegen-sql :default [x _]
  (if (and (symbol? x) (nil? (namespace x)))
    (with-meta (symbol-suffix x) (meta x))
    x))

;; Experimental key prefix decorators for map (tuple) and set (relation).

(deftype MapWithPrefix [m prefix]
  Associative
  (entryAt [_ k]
    (when (= prefix (namespace k))
      (when-let [e (find m (keyword (name k)))]
        (MapEntry/create (keyword prefix (name k)) (val e)))))
  ILookup
  (valAt [_ k]
    (when (= prefix (namespace k))
      (get m (keyword (name k)))))
  IReduceInit
  (reduce [this f init]
    (transduce (map (fn [[k v]]
                      (MapEntry/create (keyword prefix (name k)) v)))
               (completing f) init m))
  IPersistentCollection
  (cons [this x]
    (merge (into {} this) x))
  Seqable
  (seq [_]
    (for [[k v] m]
      (MapEntry/create (keyword prefix (name k)) v)))
  Object
  (toString [this]
    (str (into {} this))))

(deftype SetWithPrefix [xrel prefix]
  Counted
  (count [_]
    (count xrel))
  IPersistentSet
  (disjoin [_ x]
    (SetWithPrefix. (disj xrel (.m ^MapWithPrefix x)) prefix))
  IReduceInit
  (reduce [this f init]
    (transduce (map #(MapWithPrefix. % prefix)) (completing f) init xrel))
  Seqable
  (seq [_]
    (for [m xrel]
      (MapWithPrefix. m prefix)))
  Object
  (toString [this]
    (str (into #{} this))))

(defn codegen-predicate [pred {:keys [db-var] :as ctx}]
  (let [[new-vars ctx] (extend-scope pred ctx)]
    `(fn [{:strs ~new-vars}]
       ~(codegen-sql pred ctx))))

(defn codegen-from-where [{:keys [from where]} {:keys [db db-var result-var known-vars] :as ctx}]
  (let [known-tables (find-known-tables from)
        joins (find-joins where known-tables)
        joined-tables (set (mapcat #(map % [:lhs :rhs]) joins))
        unjoined-tables (set/difference known-tables joined-tables)
        cross-products (for [table unjoined-tables]
                         {:lhs table :using {}})
        join-selections (set (map :selection joins))
        all-joins (concat cross-products (calculate-join-order db joins))
        where (set/difference (normalize-where where) join-selections)
        base-table->selection (find-base-table->selections where ctx)
        selections (set/difference where (reduce set/union (vals base-table->selection)))
        add-base-selection (fn [x]
                             (if-let [selection (get base-table->selection x)]
                               (list `set/select (codegen-predicate (vec (cons :and selection)) ctx) x)
                               x))]
    `(let [~(->> (for [[table as] from
                       :when (not (sub-query? table))]
                   [as (str table)])
                 (into {}))
           ~db-var
           ~@(->> (for [[table as] from
                        :when (sub-query? table)]
                    (cond-> [as (codegen-sql table ctx)]
                      (get base-table->selection as) (conj as (add-base-selection as))))
                  (reduce into []))]
       (as-> #{}
           ~result-var
           ~@(loop [[join :as all-joins] all-joins
                    acc []
                    joined-rels #{}
                    selections selections]
               (if join
                 (let [{:keys [lhs rhs using] :as join} (if (or (nil? (:using join)) (empty? joined-rels))
                                                          join
                                                          (first (for [{:keys [lhs rhs] :as join} all-joins
                                                                       :when (not-empty (set/intersection joined-rels #{lhs rhs}))]
                                                                   join)))
                       joins (remove #{join} all-joins)
                       other-joins (set (for [join joins
                                              :when (or (and (= lhs (:lhs join))
                                                             (contains? joined-rels (:rhs join)))
                                                        (and (= rhs (:rhs join))
                                                             (contains? joined-rels (:lhs join))))]
                                          join))
                       joins (remove other-joins joins)
                       using (->> (for [[lc rc] (apply merge using (map :using other-joins))]
                                    [(str (symbol-suffix lc))
                                     (str (symbol-suffix rc))])
                                  (into {}))
                       acc (cond
                             (and (nil? rhs) (empty? joined-rels))
                             (conj acc (add-base-selection lhs))
                             (or (nil? rhs) (contains? joined-rels rhs))
                             (conj acc `(set/join ~(add-base-selection lhs) ~result-var ~using))
                             (contains? joined-rels lhs)
                             (conj acc `(set/join ~result-var ~(add-base-selection rhs) ~using))
                             :else
                             (conj acc (cond->> `(set/join ~(add-base-selection lhs) ~(add-base-selection rhs) ~using)
                                         (not-empty joined-rels) (list 'set/join result-var))))
                       joined-rels (conj joined-rels lhs (or rhs lhs))
                       new-selections (set (filter
                                            (fn [s]
                                              (set/superset? joined-rels
                                                             (set (for [v (find-free-vars s)
                                                                        :when (not (contains? known-vars v))]
                                                                    (symbol-prefix v)))))
                                            selections))]
                   (recur joins
                          (cond-> acc
                            (seq new-selections) (conj `(set/select ~(codegen-predicate (vec (cons :and new-selections)) ctx) ~result-var)))
                          joined-rels
                          (set/difference selections new-selections)))
                 (cond-> acc
                   (seq selections) (conj `(set/select ~(codegen-predicate (vec (cons :and selections)) ctx) ~result-var)))))))))

(defn codegen-select [{:keys [select scalar-sub-query? row-sub-query?]} {:keys [result-var] :as ctx}]
  (let [[new-vars ctx] (extend-scope select ctx)]
    (cond
      (= [:star] select)
      result-var
      scalar-sub-query?
      `(when-let [{:strs ~new-vars} (first ~result-var)]
         ~(codegen-sql (ffirst select) ctx))
      row-sub-query?
      `(map (fn [{:strs ~new-vars}]
              ~(codegen-sql (ffirst select) ctx))
            ~result-var)
      :else
      `(into #{}
             (map (fn [{:strs ~new-vars}]
                    (hash-map
                     ~@(->> (for [[exp as] select]
                              [(str (symbol-suffix as))
                               (codegen-sql exp ctx)])
                            (reduce into [])))))
             ~result-var))))

(defn codegen-group-by [{:keys [select group-by having scalar-sub-query? row-sub-query?]} {:keys [result-var known-vars] :as ctx}]
  (let [group-var (gensym 'group)
        all-groups-var (gensym 'all-groups)
        ctx (assoc ctx :group-var group-var)]
    `(let [~all-groups-var ~(if (empty? group-by)
                              `(remove empty? [(seq ~result-var)])
                              `(->> (group-by (fn [{:strs ~(mapv symbol-suffix group-by)}]
                                                ~(mapv symbol-suffix group-by))
                                              ~result-var)
                                    (vals)
                                    (remove empty?)))
           ~all-groups-var ~(if having
                              (let [[new-vars ctx] (extend-scope group-by ctx)]
                                `(filter (fn [[{:strs ~new-vars} :as ~group-var]]
                                           ~(codegen-sql having ctx))
                                         ~all-groups-var))
                              all-groups-var)]
       ~(let [[new-vars ctx] (extend-scope group-by ctx)]
          (cond
            scalar-sub-query?
            `(when-let [[{:strs ~new-vars} :as ~group-var] (first ~all-groups-var)]
               ~(codegen-sql (ffirst select) ctx))
            row-sub-query?
            `(map (fn [[{:strs ~new-vars} :as ~group-var]]
                    ~(codegen-sql (ffirst select) ctx))
                  ~all-groups-var)
            :else
            `(into #{} (map (fn [[{:strs ~new-vars} :as ~group-var]]
                              (hash-map
                               ~@(->> (for [[exp as] select]
                                        [(str (symbol-suffix as))
                                         (codegen-sql exp ctx)])
                                      (reduce into [])))))
                   ~all-groups-var))))))

(defn codegen-order-by [{:keys [order-by]} {:keys [result-var]}]
  `(sort (-> ~@(reduce
                (fn [acc [col dir]]
                  (cond->> `(Comparator/comparing
                             (reify Function
                               (apply [_# {:strs [~(symbol-suffix col)]}]
                                 ~(symbol-suffix col))))
                    (= :desc dir) (list '.reversed)
                    (not-empty acc) (list '.thenComparing)
                    true (conj acc)))
                []
                order-by))
         ~result-var))

(defn codegen-offset-limit [{:keys [offset limit]} {:keys [result-var]}]
  `(into [] (comp ~@(concat
                     (when offset
                       [`(drop ~offset)])
                     (when limit
                       [`(take ~limit)])))
         ~result-var))

(defn contains-set-function? [x]
  (let [acc (volatile! false)]
    (w/postwalk (fn [x]
                  (when (and (vector? x) (contains? #{:count :avg :sum :min :max} (first x)))
                    (vreset! acc true))
                  x) x)
    @acc))

(defn maybe-add-group-by [{:keys [group-by select] :as q}]
  (if (and (nil? group-by) (contains-set-function? select))
    (assoc q :group-by [])
    q))

(defn rewrite-correlated-subqueries [x]
  (let [rewrites (volatile! #{})]
    (w/prewalk (fn [x]
                 (when (and (vector? x) (= :exists (first x)))
                   (let [[_ sub-query] x
                         {:keys [where select] :as query} (query->map sub-query)
                         known-vars (set/difference (find-free-vars where) (find-free-vars sub-query))
                         tmp (gensym 'tmp)
                         selection-smap (->> (for [v known-vars]
                                               [v (symbol (str tmp "." (symbol-suffix v)))])
                                             (into {}))
                         selections (set (mapcat val (find-base-table->selections where {:known-vars #{}})))
                         correlated-selections (set/difference (normalize-where where) selections)]
                     (when (and (= [:star] select)
                                (= 1 (count correlated-selections)))
                       (vswap! rewrites conj
                               [{x (w/postwalk-replace selection-smap (first correlated-selections))}
                                [(map->query
                                  (assoc query
                                         :where (vec selections)
                                         :select (vec (for [v (filter known-vars (find-free-vars correlated-selections))]
                                                        [v (symbol-suffix v)]))))
                                 tmp]]))))
                 x)
               (:where x))
    (reduce
     (fn [acc [where-smap from]]
       (-> acc
           (update :where #(w/postwalk-replace where-smap %))
           (update :from conj from)))
     x @rewrites)))

(defmethod codegen-sql :select-exp [query {:keys [db-var scalar-sub-query? row-sub-query?] :as ctx}]
  (let [{:keys [group-by order-by offset limit] :as query} (maybe-add-group-by (query->map query))
        result-var (gensym 'result)
        query (assoc query :scalar-sub-query? scalar-sub-query? :row-sub-query? row-sub-query?)
        ctx (assoc ctx :result-var result-var)
        ctx (dissoc ctx :scalar-sub-query? :row-sub-query?)]
    `(as-> #{} ~result-var
       ~@(cond-> [(codegen-from-where query ctx)]
           group-by (conj (codegen-group-by query ctx))
           (nil? group-by) (conj (codegen-select query ctx))
           order-by (conj (codegen-order-by query ctx))
           (or offset limit) (conj (codegen-offset-limit query ctx))))))

(defmethod codegen-sql :union [[_ lhs rhs] ctx]
  `(set/union ~(codegen-sql lhs ctx) ~(codegen-sql rhs ctx)))

(defmethod codegen-sql :except [[_ lhs rhs] ctx]
  `(set/difference ~(codegen-sql lhs ctx) ~(codegen-sql rhs ctx)))

(defmethod codegen-sql :intersect [[_ lhs rhs] ctx]
  `(set/intersect ~(codegen-sql lhs ctx) ~(codegen-sql rhs ctx)))

(defmethod codegen-sql :with-exp [with-exp {:keys [db] :as ctx}]
  (let [nonjoin-exp (last with-exp)
        with-spec (when (= 3 (count with-exp))
                    (second with-exp))
        db-var (gensym 'db)
        ctx {:db db :db-var db-var :known-vars #{}}]
    `(fn [~db-var]
       (let [~@(->> (for [[table-name table-subquery] with-spec]
                      [table-name (codegen-sql table-subquery ctx)])
                    (reduce into []))]
         ~(codegen-sql nonjoin-exp ctx)))))

(defn build-column->tables [db]
  (->> (for [{:keys [name columns]} (map meta (vals db))
             c (keys columns)]
         {c #{name}})
       (apply merge-with set/union)))

(defn parse-and-transform [sql column->tables]
  (qualify-transform
   (reduce
    (fn [acc transform-map]
      (insta/transform transform-map acc))
    (parse-sql sql)
    [literal-transform
     constant-folding-transform
     nary-transform
     simplify-transform
     normalize-transform])
   column->tables))

(def parse-and-transform-memo (memoize parse-and-transform))

(defn parse-and-transform-sql [sql db]
  (parse-and-transform-memo sql (build-column->tables db)))

(defn sql->clj [sql db]
  (codegen-sql (parse-and-transform-sql sql db)  {:db db}))

(defn compile-sql [sql db]
  (eval (sql->clj sql db)))

(defn execute-sql [sql db]
  ((compile-sql sql db) db))

(comment
  (for [q (map inc (range 22))]
    (parse-sql (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" q)))))

  ((eval
     (codegen-sql
      (parse-and-transform
       (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 2))))
      crux.tpch/db-sf-0_01))
   crux.tpch/db-sf-0_01))

;; High level SQL grammar, from
;; https://calcite.apache.org/docs/reference.html

;; See also Date, SQL and Relational Theory, p. 455-458, A Simplified
;; BNF Grammar

;; SQLite grammar:
;; https://github.com/bkiers/sqlite-parser/blob/master/src/main/antlr4/nl/bigo/sqliteparser/SQLite.g4
;; https://www.sqlite.org/lang_select.html

;; SQL BNF from the spec:
;; https://ronsavage.github.io/SQL/

;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/frontend/parser/SQLAST.scala
;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/frontend/parser/SQLParser.scala

;; RelaX - relational algebra calculator
;; https://dbis-uibk.github.io/relax/

;; https://www.academia.edu/2280992/Translating_SQL_Into_Relational_Algebra_Optimization_Semantics_and_Equivalence_of_SQL_Queries
;; https://www.cs.purdue.edu/homes/rompf/papers/rompf-icfp15.pdf

;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/queryengine/push/Operators.scala
;; https://github.com/epfldata/dblab/blob/develop/components/src/main/scala/ch/epfl/data/dblab/queryengine/volcano/Operators.scala
