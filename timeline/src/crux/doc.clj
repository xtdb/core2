(ns crux.doc
  (:require [clojure.spec.alpha :as s]))

(s/def ::crux-cardinality-one
  (s/or :boolean boolean?
        :string string?
        :double double?
        :long int?
        :date inst?
        :keyword keyword?
        :nil nil?
        :doc ::crux-doc))

(s/def ::crux-cardinality-many
  (s/or :vector (s/coll-of ::crux-cardinality-one :kind vector?)
        :set (s/coll-of ::crux-cardinality-one :kind set?)))

(s/def ::crux-value (s/or :one ::crux-cardinality-one
                          :many ::crux-cardinality-many))

(s/def ::crux-doc (s/map-of keyword? ::crux-value))

(s/def ::json-value (s/or :boolean boolean?
                          :number number?
                          :string string?
                          :null nil?
                          :array (s/coll-of ::json-value :kind vector?)
                          :object ::json-object))

(s/def ::json-object (s/map-of string? ::json-value))
