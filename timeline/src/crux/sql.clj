(ns crux.sql
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.instant :as i]
            [clojure.string :as s]
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
                      (and (number? x) (number? y))
                      (- x y)
                      :else
                      [:numeric-minus x y]))
   :numeric-plus (fn [x y]
                   (cond
                     (and (instance? Date x)
                          (instance? TemporalAmount y))
                     (Date/from (.toInstant (.plus (.atOffset (.toInstant ^Date x) ZoneOffset/UTC) ^TemporalAmount y)))
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

(defn symbol-suffix [x]
  (symbol (s/replace x #"^.+\." "")))

(def normalize-transform
  (merge
   {:table-spec (fn [x & [y]]
                  [x (or y x)])
    :select-item (fn [x & [y]]
                   [x (or y (if (symbol? x)
                              (symbol-suffix x)
                              (gensym "column_")))])
    :sort-spec (fn [x & [dir]]
                 [x (or dir :asc)])
    :set-function-spec (fn [& args]
                         (vec args))
    :select-exp (fn [& args]
                  (let [select (zipmap (map first args)
                                       (map (comp vec rest) args))]
                    (reduce
                     (fn [acc k]
                       (cond-> acc
                         (contains? acc k)
                         (update k first)))
                     select [:where :having :offset :limit])))}
   (let [constants [:count :sum :avg :min :max :star :asc :desc]]
     (zipmap constants (map constantly constants)))))

(defn qualify-transform [column->tables]
  {:identifier (fn [x]
                 (if-let [ts (get column->tables (name x))]
                   (if (= 1 (count ts))
                     (symbol (str (first ts) "." (name x)))
                     (throw (IllegalArgumentException. (str "Column not unique:" x))))
                   (symbol x)))})

(def codegen-transform
  {:boolean-not (fn [x]
                  `(not ~x))
   :boolean-and (fn [& xs]
                  `(and ~@xs))
   :boolean-or (fn [& xs]
                 `(or ~@xs))
   :comp-eq (fn [x y]
              `(= ~x ~y))
   :comp-ne (fn [x y]
              `(not= ~x ~y))
   :comp-lt (fn [x y]
              (if (or (number? x) (number? y))
                `(< ~x ~y)
                `(neg? (compare ~x ~y))))
   :comp-le (fn [x y]
              (if (or (number? x) (number? y))
                `(<= ~x ~y)
                `(not (pos? (compare ~x ~y)))))
   :comp-gt (fn [x y]
              (if (or (number? x) (number? y))
                `(> ~x ~y)
                `(pos? (compare ~x ~y))))
   :comp-ge (fn [x y]
              (if (or (number? x) (number? y))
                `(>= ~x ~y)
                `(not (neg? (compare ~x ~y)))))
   :numeric-plus (fn [x y]
                   `(+ ~x ~y))
   :numeric-minus (fn [x y]
                    `(- ~x ~y))
   :numeric-multiply (fn [x y]
                       `(* ~x ~y))
   :numeric-divide (fn [x y]
                     `(/ ~x ~y))
   :numeric-modulo (fn [x y]
                     `(rem ~x ~y))
   :like-exp (fn [x pattern]
               `(boolean (re-find ~pattern ~x)))
   :case-exp (fn [cond then else]
               `(if ~cond ~then ~else))
   :extract-exp (fn [[field] x]
                  `(.get (.atOffset (.toInstant ~x) ZoneOffset/UTC)
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
                           `(~f ~@args)))})

(comment
  (for [q (map inc (range 22))]
    (parse-sql (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" q)))))

  (reduce
   (fn [acc transform-map]
     (insta/transform transform-map acc))
   (parse-sql (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 16))))
   [(qualify-transform (crux.tpch/build-column->tables crux.tpch/db-sf-0_01))
    literal-transform
    constant-folding-transform
    nary-transform
    normalize-transform]))

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
