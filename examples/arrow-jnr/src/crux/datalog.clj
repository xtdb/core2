(ns crux.datalog
  (:require [clojure.spec.alpha :as s]))

;; See Racket for the Datalog syntax in EBNF.
;; https://docs.racket-lang.org/datalog/datalog.html

(defn- prolog-var? [s]
  (and (symbol? s)
       (Character/isUpperCase (char (first (name s))))))

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
                               :colon-hypen #{:- 'is}
                               :symbol ::identifier
                               :terms (s/? (s/coll-of ::term :kind list?))))
(s/def ::not (s/cat :exlamation-mark '#{! not}
                    :predicate ::predicate))
(s/def ::literal (s/alt :predicate ::predicate
                        :equality-predicate ::equality-predicate
                        :external-query ::external-query
                        :arithmetic ::arithmetic
                        :not ::not))
(s/def ::term (s/or :variable ::variable
                    :constant ::constant))

(s/def ::constant (complement (some-fn list? prolog-var?)))
(s/def ::identifier (s/and symbol? (complement (some-fn prolog-var? '#{. ? = != ! % not}))))
(s/def ::variable prolog-var?)
