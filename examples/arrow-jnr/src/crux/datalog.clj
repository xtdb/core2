(ns crux.datalog
  (:require [clojure.edn :as edn]
            [clojure.java.io :as  io]
            [clojure.spec.alpha :as s]
            [instaparse.core :as insta])
  (:import clojure.lang.LineNumberingPushbackReader
           java.io.StringReader))

;; See Racket for the Datalog syntax in EBNF.
;; https://docs.racket-lang.org/datalog/datalog.html

;; See also Mitre Datalog:
;; https://github.com/TheKnarf/mitre-datalog-fork/blob/master/src/datalog.lua.in

(defn logic-var? [s]
  (and (symbol? s)
       (let [c (char (first (name s)))]
         (or (= \_ c) (Character/isUpperCase c)))))

(s/def ::program (s/* ::statement))
(s/def ::statement (s/alt :assertion ::assertion
                          :retraction ::retraction
                          :query ::query
                          :requirement ::requirement))
(s/def ::assertion (s/cat :clause ::clause
                          :dot #{'.}))
(s/def ::retraction (s/cat :clause ::clause
                           :tilde #{'-}))
(s/def ::query (s/cat :head ::predicate
                      :question-mark #{'?}))
(s/def ::rule (s/cat :head ::rule-head
                     :colon-hypen #{:-}
                     :body ::body))
(s/def ::rule-head (s/cat :symbol ::identifier
                          :terms (s/? (s/spec (s/* ::head-term)))))

(s/def ::requirement (s/cat :identifier (s/coll-of ::identifier :kind list? :count 1)
                            :dot #{'.}))
(s/def ::fact (s/cat :head ::predicate))
(s/def ::clause (s/alt :rule ::rule
                       :fact ::fact))
(s/def ::body (s/+ ::literal))
(s/def ::predicate (s/cat :symbol ::identifier
                          :terms (s/? (s/spec (s/* ::term)))))
(s/def ::equality-predicate (s/cat :lhs ::term
                                   :op '#{= != < <= > >=}
                                   :rhs ::term))
(s/def ::external-query (s/cat :variable ::variable
                               :colon-hypen '#{:- is}
                               :symbol ::identifier
                               :terms (s/spec (s/* ::term))))
(s/def ::not-predicate (s/cat :exclamation-mark '#{! not}
                              :predicate ::predicate))
(s/def ::literal (s/alt :predicate ::predicate
                        :equality-predicate ::equality-predicate
                        :not-predicate ::not-predicate
                        :external-query ::external-query))
(s/def ::term (s/alt :variable ::variable
                     :constant ::constant))
(s/def ::head-term (s/alt :aggregate ::aggregate
                          :variable ::variable
                          :constant ::constant))

(s/def ::aggregate (s/cat :op '#{min max count sum} :variable (s/coll-of ::variable :kind list? :count 1)))

(s/def ::constant (complement (some-fn list? coll? logic-var?)))
(s/def ::identifier (s/and (some-fn symbol? boolean?)
                           (complement (some-fn logic-var? '#{. ? = != ! % not}
                                                #(and (not= '- %) (contains? #{\. \? \-} (last (str %))))))))
(s/def ::variable logic-var?)

(defn parse-datalog [datalog-source]
  (let [in (LineNumberingPushbackReader.
            (if (string? datalog-source)
              (StringReader. ^String datalog-source)
              (io/reader datalog-source)))]
    (->> (repeatedly #(edn/read {:eof nil} in))
         (take-while identity)
         (s/assert :crux.datalog/program))))

(def ^:private datalog-whitespace-ebnf
  "whitespace = #'[,\\s]+' | whitespace? #'%.+(\n|$)' whitespace?")

(def ^:private
  datalog-parser
  (insta/parser
   (io/resource "crux/datalog.ebnf")
   :auto-whitespace
   (insta/parser datalog-whitespace-ebnf)))

(defn parse-ebnf-datalog [datalog-source]
  (->> (insta/parse datalog-parser datalog-source)
       (insta/transform
        {:term (fn [[tag value]]
                 [tag (edn/read-string value)])
         :assignment (fn [[_ variable] value]
                       [:assignment (symbol variable) value])
         :equality_predicate (fn [x op y]
                               [:equality_predicate op x y])
         :comparison_operator symbol
         :symbol symbol
         :external_symbol symbol
         :arguments vector})))
