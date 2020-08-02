(ns crux.sql
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.instant :as i]
            [clojure.string :as s]
            [instaparse.core :as insta])
  (:import [java.time Duration Period]))

(defn parse-string [x]
  (s/replace (subs x 1 (dec (count x)))
             "''"
             "'"))

(defn parse-date [x]
  (i/read-instant-date (parse-string x)))

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
   :interval-literal parse-interval
   :string-literal parse-string
   :like-pattern parse-like-pattern
   :identifier parse-identifier})

(comment
  (for [q (map inc (range 22))]
    (parse-sql (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" q))))))

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
