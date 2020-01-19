(ns crux.datalog
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [instaparse.core :as insta]))

;; See Racket for the Datalog syntax in EBNF.
;; https://docs.racket-lang.org/datalog/datalog.html

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
(s/def ::rule (s/cat :head ::predicate
                     :colon-hypen #{:-}
                     :body ::body))
(s/def ::requirement (s/cat :identifier (s/coll-of ::identifier :kind list? :count 1)
                            :dot #{'.}))
(s/def ::fact (s/cat :head ::predicate))
(s/def ::clause (s/alt :rule ::rule
                       :fact ::fact))
(s/def ::body (s/+ ::literal))
(s/def ::predicate (s/cat :symbol ::identifier
                          :terms (s/? (s/coll-of ::term :kind list?))))
(s/def ::equality-predicate (s/cat :lhs ::term
                                   :op '#{= != < <= > >=}
                                   :rhs ::term))
(s/def ::external-query (s/cat :variable ::variable
                               :colon-hypen '#{:- is}
                               :symbol ::identifier
                               :terms (s/? (s/coll-of ::term :kind list?))))
(s/def ::not-predicate (s/cat :exclamation-mark '#{! not}
                              :predicate ::predicate))
(s/def ::literal (s/alt :predicate ::predicate
                        :equality-predicate ::equality-predicate
                        :not-predicate ::not-predicate
                        :external-query ::external-query))
(s/def ::term (s/or :variable ::variable
                    :constant ::constant))

(s/def ::constant (complement (some-fn list? logic-var?)))
(s/def ::identifier (s/and (some-fn symbol? boolean?)
                           (complement (some-fn logic-var? '#{. ? = != ! % not}
                                                #(and (not= '- %) (contains? #{\. \? \-} (last (str %))))))))
(s/def ::variable logic-var?)

;; real parser, not complete.
;; requires https://github.com/Engelberg/instaparse
;; non-trivial example: https://github.com/bodil/BODOL/blob/master/src/bodol/parser.clj

(def ^:private
  datalog-parser
  (insta/parser
   "
<program> = statement*
<statement> = assertion | retraction | query | requirement
assertion = clause <'.'>
retraction = clause <('~'|'-')>
requirement = <'('> symbol <')'> <'.'>
query = predicate <'?'>
<clause> = rule | fact
rule =  head body
fact = predicate
head = predicate <':-'>
body = literal*
<literal> = predicate | not | equality_predicate | assignment
predicate = symbol arguments?
not = <('not'|'!')> predicate
external_query = symbol arguments?
equality_predicate = term comparison_operator term
comparison_operator = '=' | '!=' | '<' | '<=' | '>' | '>='
assignment = variable <('is'|':-'|'=')> (arithmetic | external_query)
<arithmetic> = mul_div | add | sub
add = arithmetic <'+'> mul_div
sub = arithmetic <'-'> mul_div
<mul_div> = expression_term | mul | div
mul = mul_div <'*'> expression_term
div = mul_div <'/'> expression_term
<expression_term> = term | <'('> arithmetic <')'>
symbol = identifier
arguments = (<'('> term* <')'>)
term = constant | variable
variable = #'[A-Z_]\\w*'
constant = boolean |  identifier | string | number
<boolean> = 'true' | 'false'
<string> = #'\"([^\"\\\\]|\\\\.)*\"'
<number> = #'-?\\d+(.?\\d+)?'
<identifier> = #'[a-z]\\w*'"
   :auto-whitespace
   (insta/parser "
whitespace = #'[,\\s]+' | comment
comment = whitespace? #'%.+(\n|$)' whitespace?")))

(defn parse-datalog [datalog-source]
  (insta/transform
   {:term (fn [[tag value]]
            [tag (edn/read-string value)])
    :comparison_operator symbol
    :variable symbol
    :symbol symbol
    :arguments vector}
   (insta/parse datalog-parser datalog-source)))
