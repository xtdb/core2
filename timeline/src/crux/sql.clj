(ns crux.sql
  (:require [clojure.java.io :as io]
            [instaparse.core :as insta]))

(def parse-sql
  (insta/parser (io/resource "crux/sql.ebnf")
   :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*' | #'\\s*/[*].*([*]/\\s*|$)'")
   :string-ci true))

;; High level SQL grammar, from
;; https://calcite.apache.org/docs/reference.html

;; See also Date, SQL and Relational Theory, p. 455-458, A Simplified
;; BNF Grammar

;; SQLite grammar:
;; https://github.com/bkiers/sqlite-parser/blob/master/src/main/antlr4/nl/bigo/sqliteparser/SQLite.g4
;; https://www.sqlite.org/lang_select.html

;; SQL BNF from the spec:
;; https://ronsavage.github.io/SQL/

;; RelaX - relational algebra calculator
;; https://dbis-uibk.github.io/relax/
