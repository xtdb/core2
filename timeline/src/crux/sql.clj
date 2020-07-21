(ns crux.sql
  (:require [clojure.java.io :as io]
            [instaparse.core :as insta]))

(def sql
  (insta/parser (io/resource "crux/sql.ebnf")
   :auto-whitespace :comma
   :string-ci true))
