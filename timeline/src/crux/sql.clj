(ns crux.sql
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.instant :as i]
            [clojure.set :as set]
            [clojure.string :as s]
            [clojure.walk :as w]
            [instaparse.core :as insta])
  (:import [java.util Comparator Date]
           java.util.function.Function
           [java.time Duration Period ZoneOffset]
           [java.time.temporal ChronoField TemporalAmount]))

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

(def constant-folding-transform
  {:numeric-minus (fn [x y]
                    (cond
                      (and (instance? Date x)
                           (instance? TemporalAmount y))
                      (Date/from (.toInstant (.minus (.atOffset (.toInstant ^Date x) ZoneOffset/UTC) ^TemporalAmount y)))
                      (and (double? x) (double? y))
                      (double (- (bigdec x) (bigdec y)))
                      (and (number? x) (number? y))
                      (- x y)
                      :else
                      [:numeric-minus x y]))
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
                  (if (vector? x)
                    [x y]
                    [x (or y x)]))
    :select-item (fn [x & [y]]
                   [x (or y (if (symbol? x)
                              (symbol-suffix x)
                              (gensym "column_")))])
    :sort-spec (fn [x & [dir]]
                 [x (or dir :asc)])
    :set-function-spec (fn [& args]
                         (vec args))}
   (let [constants [:count :sum :avg :min :max :star :distinct :asc :desc :year :month :day :hour :minute]]
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

(defn qualify-transform [column->tables]
  {:identifier (fn [x]
                 (if-let [ts (get column->tables (name x))]
                   (if (= 1 (count ts))
                     (symbol (str (first ts) "." (name x)))
                     (throw (IllegalArgumentException. (str "Column not unique:" x))))
                   (symbol x)))})

(defn query->map [[_ & args]]
  (let [select (zipmap (map first args)
                       (map (comp vec rest) args))]
    (reduce
     (fn [acc k]
       (cond-> acc
         (contains? acc k)
         (update k first)))
     select [:where :having :offset :limit])))

(defn symbol-suffix [x]
  (symbol (s/replace x #"^.+\." "")))

(defn symbol-prefix [x]
  (symbol (s/replace x #"\..+$" "")))

(defn symbol-with-prefix? [x]
  (boolean (re-find #"\." (str x))))

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
             (* (count (get db (str lhs)))
                (count (get db (str rhs)))))
           joins))

(defn sub-query? [x]
  (and (vector? x )
       (contains? #{:union :except :intersect :select-exp} (first x))))

(defn find-base-table->selections [where]
  (->> (for [x (normalize-where where)
             :when (and (= 1 (count (distinct (map symbol-prefix (filter symbol? x)))))
                        (not-any? sub-query? x))
             :let [[a] (filter symbol? x)]]
         {(symbol-prefix a) #{x}})
       (apply merge-with set/union)))

(defn remove-symbol-prefixes [x]
  (w/postwalk #(if (and (symbol? %) (nil? (namespace %)))
                 (symbol-suffix %)
                 %) x))

(defn find-free-vars [x known-tables]
  (let [acc (volatile! #{})]
    (w/postwalk #(do (when (and (symbol? %) (nil? (namespace %))
                                (symbol-with-prefix? %)
                                (not (contains? known-tables (symbol-prefix %))))
                       (vswap! acc conj (symbol-suffix %)))
                     %) x)
    @acc))

(defn find-known-tables [from]
  (set (filter symbol? (map second from))))

(defn find-symbol-suffixes [x]
  (let [acc (volatile! #{})]
    (w/prewalk #(do (when (and (symbol? %) (nil? (namespace %)))
                      (vswap! acc conj (symbol-suffix %)))
                    (if (sub-query? %)
                      (let [known-tables (find-known-tables (:from (query->map %)))]
                        (vswap! acc set/union (find-free-vars % known-tables)))
                      %)) x)
    @acc))

(defn maybe-sub-query [x]
  (if (::sub-query x)
    `(->> ~x (ffirst) (val))
    x))

(def codegen-transform
  {:not (fn [x]
          `(not ~x))
   :and (fn [& xs]
          `(and ~@xs))
   :or (fn [& xs]
         `(or ~@xs))
   := (fn [x y]
        `(= ~(maybe-sub-query x) ~(maybe-sub-query y)))
   :<> (fn [x y]
         `(not= ~(maybe-sub-query x) ~(maybe-sub-query y)))
   :< (fn [x y]
        (if (or (number? x) (number? y))
          `(< ~(maybe-sub-query x) ~(maybe-sub-query y))
          `(neg? (compare ~(maybe-sub-query x) ~(maybe-sub-query y)))))
   :<= (fn [x y]
         (if (or (number? x) (number? y))
           `(<= ~(maybe-sub-query x) ~(maybe-sub-query y))
           `(not (pos? (compare ~(maybe-sub-query x) ~(maybe-sub-query y))))))
   :> (fn [x y]
        (if (or (number? x) (number? y))
          `(> ~(maybe-sub-query x) ~(maybe-sub-query y))
          `(pos? (compare ~(maybe-sub-query x) ~(maybe-sub-query y)))))
   :>= (fn [x y]
         (if (or (number? x) (number? y))
           `(>= ~(maybe-sub-query x) ~(maybe-sub-query y))
           `(not (neg? (compare ~(maybe-sub-query x) ~(maybe-sub-query y))))))
   :+ (fn [x y]
        `(+ ~x ~y))
   :- (fn [x y]
        `(- ~x ~y))
   :* (fn [x y]
        `(* ~x ~y))
   :/ (fn [x y]
        `(/ ~x ~y))
   :% (fn [x y]
        `(rem ~x ~y))
   :exists (fn [x]
             `(not-empty ~x))
   :unique (fn [x]
             `(apply distinct? ~x))
   :in (fn [x y]
         (if (sub-query? y)
           `(some #{x} y)
           `(contains? y x)))
   :all (fn [[x op] y]
          (let [var-sym (gensym 'var)]
            `(every? #(~@(insta/transform codegen-transform [op x '%]))
                     (map (comp val first) ~y))))
   :any (fn [[x op] y]
          `(boolean (some #(~@(insta/transform codegen-transform [op x '%]))
                          (map (comp val first) ~y))))
   :like (fn [x pattern]
           `(boolean (re-find ~pattern ~x)))
   :case (fn [cond then else]
           `(if ~cond ~then ~else))
   :extract (fn [field x]
              `(.get (.atOffset (.toInstant ~(if (symbol? x)
                                               (with-meta x {:tag `Date})
                                               x)) ZoneOffset/UTC)
                     ~(case field
                        :year `ChronoField/YEAR
                        :month `ChronoField/MONTH_OF_YEAR
                        :day `ChronoField/DAY_OF_MONTH
                        :hour `ChronoField/HOUR_OF_DAY
                        :minute `ChronoField/MINUTE_OF_HOUR)))
   :routine-invocation (fn [f & args]
                         (case f
                           (substr
                            substring) (let [[x start length] args]
                            `(subs ~x (dec ~start) (+ (dec ~start) ~length)))
                           `(~f ~@args)))
   :sum (fn [x]
          `(fn [group#]
             (reduce
              (fn [acc# {:strs ~(vec (find-symbol-suffixes x))}]
                (+ acc# ~(remove-symbol-prefixes x)))
              0 group#)))
   :avg (fn [x]
          `(fn [group#]
             (/ (reduce
                 (fn [acc# {:strs ~(vec (find-symbol-suffixes x))}]
                   (+ acc# ~(remove-symbol-prefixes x)))
                 0 group#)
                (count group#))))
   :min (fn [x]
          `(fn [group#]
             (reduce
              (fn [acc# {:strs ~(vec (find-symbol-suffixes x))}]
                (min acc# ~(remove-symbol-prefixes x)))
              Long/MAX_VALUE group#)))
   :max (fn [x]
          `(fn [group#]
             (reduce
              (fn [acc# {:strs ~(vec (find-symbol-suffixes x))}]
                (max acc# ~(remove-symbol-prefixes x)))
              Long/MIN_VALUE group#)))
   :count (fn [x]
            (if (= :star x)
              `(fn [group#]
                 (count group#))
              `(fn [group#]
                 (count (map (fn [{:strs ~(vec (find-symbol-suffixes x))}]
                               ~(remove-symbol-prefixes x))
                             group#)))))})

(defn maybe-add-group-by [{:keys [group-by select] :as q}]
  (if (or group-by (= [:star] select) (every? symbol? (map first select)))
    q
    (assoc q :group-by [])))

(declare codegen-sql)

(defn codegen-exp [exp {:keys [known-vars db-var] :as ctx}]
  (remove-symbol-prefixes
   (w/postwalk #(if-let [sub-query (::sub-query %)]
                  (codegen-sql sub-query ctx)
                  %)
               (insta/transform
                codegen-transform
                (w/postwalk #(if (sub-query? %)
                               {::sub-query %}
                               %)
                            exp)))))

(defn codegen-predicate [pred {:keys [known-vars db-var] :as ctx}]
  `(fn [{:strs ~(vec (remove known-vars (find-symbol-suffixes pred)))}]
     ~(let [ctx (update ctx :known-vars set/union (find-symbol-suffixes pred))]
        (codegen-exp pred ctx))))

(defn codegen-from-where [{:keys [from where]} {:keys [db db-var known-vars] :as ctx}]
  (let [known-tables (find-known-tables from)
        joins (find-joins where known-tables)
        joined-tables (set (mapcat #(map % [:lhs :rhs]) joins))
        unjoined-tables (set/difference known-tables joined-tables)
        join-selections (set (map :selection joins))
        where (set/difference (normalize-where where) join-selections)
        base-table->selection (find-base-table->selections where)
        final-selection (set/difference where (reduce set/union (vals base-table->selection)))
        result-var (gensym 'result)
        add-base-selection (fn [x]
                             (if-let [selection (get base-table->selection x)]
                               (list `set/select (codegen-predicate (vec (cons :and selection)) ctx) x)
                               x))]
    `(fn [~(->> (for [[table as] from
                      :when (not (sub-query? table))]
                  [as (str table)])
                (into {:as db-var}))]
       (let [~@(->> (for [[table as] from
                          :when (sub-query? table)]
                      (cond-> [as (codegen-sql table ctx)]
                        (get base-table->selection as) (conj as (add-base-selection as))))
                    (reduce into []))]
         ~(cond->> `(as-> #{}
                        ~result-var
                        ~@(when (seq unjoined-tables)
                            (cons (add-base-selection (first unjoined-tables))
                                  (for [table (rest unjoined-tables)]
                                    `(set/join ~result-var ~(add-base-selection table)))))
                        ~@(first
                           (reduce
                            (fn [[acc joined-rels] {:keys [lhs rhs using]}]
                              (let [using (->> (for [[lc rc] using]
                                                 [(str (symbol-suffix lc))
                                                  (str (symbol-suffix rc))])
                                               (into {}))]
                                [(cond
                                   (contains? joined-rels rhs)
                                   (conj acc `(set/join ~(add-base-selection lhs) ~result-var ~using))
                                   (contains? joined-rels lhs)
                                   (conj acc `(set/join ~result-var ~(add-base-selection rhs) ~using))
                                   :else
                                   (conj acc (cond->> `(set/join ~(add-base-selection lhs) ~(add-base-selection rhs) ~using)
                                               (not-empty joined-rels) (list 'set/join result-var))))
                                 (conj joined-rels lhs rhs)]))
                            [[] #{}]
                            (calculate-join-order db joins))))
            (not-empty final-selection) (list `set/select (codegen-predicate (vec (cons :and final-selection)) ctx)))))))

(defn codegen-select [{:keys [select]} _]
  `~(if (= [:star] select)
      `identity
      `(fn [result#]
         (into #{}
               (map (fn [{:strs ~(vec (find-symbol-suffixes select))}]
                      (hash-map
                       ~@(->> (for [[exp as] select]
                                [(str (symbol-suffix as))
                                 (if (symbol? exp)
                                   (symbol-suffix exp)
                                   `~(insta/transform codegen-transform (remove-symbol-prefixes exp)))])
                              (reduce into [])))))
               result#))))

(defn codegen-group-by [{:keys [select group-by having]} ctx]
  (when group-by
    (let [result-var (gensym 'result)
          group-var (gensym 'group)
          ctx (assoc ctx :group-var group-var)]
      `(fn [~result-var]
         (->> (group-by (fn [{:strs ~(mapv symbol-suffix group-by)}]
                          ~(mapv symbol-suffix group-by))
                        ~result-var)
              (vals)
              (remove empty?)
              (into #{} (comp
                         ~@(concat
                            (when having
                              (let [ctx (update ctx :known-vars set/union (find-symbol-suffixes having))]
                                [`(filter (fn [{:strs ~(vec (find-symbol-suffixes having)) :as ~group-var}]
                                            ~(codegen-exp having ctx)))]))
                            (let [ctx (update ctx :known-vars set/union (find-symbol-suffixes select))]
                              [`(map (fn [[{:strs ~(vec (find-symbol-suffixes select))} :as ~group-var]]
                                       (hash-map
                                        ~@(->> (for [[exp as] select]
                                                 [(str (symbol-suffix as))
                                                  (if (symbol? exp)
                                                    (symbol-suffix exp)
                                                    `(~(codegen-exp exp ctx)
                                                      ~group-var))])
                                               (reduce into [])))))])))))))))

(defn codegen-order-by [{:keys [order-by]} _]
  (when order-by
    `(fn [result#]
       (sort (-> ~@(reduce
                    (fn [acc [col dir]]
                      (cond->> `(Comparator/comparing
                                 (reify Function
                                   (apply [_# {:strs [~(symbol-suffix col)]}]
                                     ~(symbol-suffix col))))
                        (= :desc dir) (list '.reversed)
                        (not-empty acc) (list '.thenComparing)
                        true (conj acc)))
                    []
                    order-by)) result#))))

(defn codegen-offset-limit [{:keys [offset limit]} _]
  (when (or offset limit)
    `(fn [result#]
       (into [] (comp ~@(concat
                         (when offset
                           [`(drop ~offset)])
                         (when limit
                           [`(take ~limit)])))
             result#))))

(defmethod codegen-sql :select-exp [query {:keys [db-var] :as ctx}]
  (let [{:keys [group-by order-by offset limit] :as query} (maybe-add-group-by (query->map query))]
    `(->> ~db-var
          ~@(cond-> [(list (codegen-from-where query ctx))]
              group-by (conj (list (codegen-group-by query ctx)))
              (nil? group-by) (conj (list (codegen-select query ctx)))
              order-by (conj (list (codegen-order-by query ctx)))
              (or offset limit) (conj (list (codegen-offset-limit query ctx)))))))

(defmulti codegen-sql (fn [[ast-type] _] ast-type))

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

(defn parse-and-transform [sql db]
  (reduce
   (fn [acc transform-map]
     (insta/transform transform-map acc))
   (parse-sql sql)
   [(qualify-transform (build-column->tables db))
    literal-transform
    constant-folding-transform
    nary-transform
    simplify-transform
    normalize-transform]))

(defn compile-sql [sql db]
  (eval (codegen-sql (parse-and-transform sql db) {:db db})))

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
