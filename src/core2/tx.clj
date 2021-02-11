(ns core2.tx
  (:import [java.util Date SortedMap]))

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])

(defrecord Watermark [^long chunk-idx ^SortedMap chunk-object-key->idx])
