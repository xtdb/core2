(ns core2.tx
  (:require [core2.util :as util])
  (:import [java.util Date SortedMap]
           [java.util.concurrent.atomic AtomicInteger]
           [java.io Closeable]))

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])

(defrecord Watermark [^long chunk-idx ^long row-count ^SortedMap column->root ^TransactionInstant tx-instant ^AtomicInteger ref-count]
  Closeable
  (close [_]
    (when (zero? (util/dec-ref-count ref-count))
      (doseq [root (vals column->root)]
        (util/try-close root)))))
