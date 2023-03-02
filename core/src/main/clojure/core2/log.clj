(ns core2.log
  (:require core2.api.impl
            [core2.util :as util]
            [clojure.tools.logging :as log])
  (:import core2.api.TransactionInstant
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           java.time.Duration
           java.util.concurrent.Semaphore))

(set! *unchecked-math* :warn-on-boxed)

(defrecord LogRecord [^TransactionInstant tx ^ByteBuffer record])

#_{:clj-kondo/ignore [:unused-binding]}
(definterface LogSubscriber
  (onSubscribe [^java.lang.AutoCloseable cancelHook])
  (acceptRecord [^core2.log.LogRecord record]))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface Log
  (^java.util.concurrent.CompletableFuture #_<LogRecord> appendRecord [^java.nio.ByteBuffer record])
  (^java.util.List #_<LogRecord> readRecords [^Long afterOffset ^int limit])
  (^void subscribe [^Long afterTxId, ^core2.log.LogSubscriber subscriber]))

(def ^java.util.concurrent.ThreadFactory subscription-thread-factory
  (util/->prefix-thread-factory "core2-tx-subscription"))

(defn- tx-handler [^LogSubscriber subscriber]
  (fn [_last-tx-id ^LogRecord record]
    (when (Thread/interrupted)
      (throw (InterruptedException.)))

    (.acceptRecord subscriber record)

    (.tx-id ^TransactionInstant (.tx record))))

(defn handle-polling-subscription [^Log log after-tx-id {:keys [^Duration poll-sleep-duration]} ^LogSubscriber subscriber]
  (doto (.newThread subscription-thread-factory
                    (fn []
                      (let [thread (Thread/currentThread)]
                        (.onSubscribe subscriber (reify AutoCloseable
                                                   (close [_]
                                                     (.interrupt thread)
                                                     (.join thread)))))
                      (try
                        (loop [after-tx-id after-tx-id]
                          (let [last-tx-id (reduce (tx-handler subscriber)
                                                   after-tx-id
                                                   (try
                                                     (.readRecords log after-tx-id 100)
                                                     (catch Exception e
                                                       (log/warn e "Error polling for txs, will retry"))))]
                            (when (Thread/interrupted)
                              (throw (InterruptedException.)))
                            (when (= after-tx-id last-tx-id)
                              (Thread/sleep (.toMillis poll-sleep-duration)))
                            (recur last-tx-id)))
                        (catch InterruptedException _))))
    (.start)))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface INotifyingSubscriberHandler
  (notifyTx [^core2.api.TransactionInstant tx])
  (subscribe [^core2.log.Log log, ^Long after-tx-id, ^core2.log.LogSubscriber subscriber]))

(defrecord NotifyingSubscriberHandler [!state]
  INotifyingSubscriberHandler
  (notifyTx [_ tx]
    (let [{:keys [semaphores]} (swap! !state assoc :latest-submitted-tx-id (.tx-id tx))]
      (doseq [^Semaphore semaphore semaphores]
        (.release semaphore))))

  (subscribe [_ log after-tx-id subscriber]
    (let [semaphore (Semaphore. 0)
          {:keys [latest-submitted-tx-id]} (swap! !state update :semaphores conj semaphore)]

      (doto (.newThread subscription-thread-factory
                        (fn []
                          (let [thread (Thread/currentThread)]
                            (.onSubscribe subscriber (reify AutoCloseable
                                                       (close [_]
                                                         (.interrupt thread)
                                                         (.join thread)))))
                          (try
                            (loop [after-tx-id after-tx-id]
                              (let [last-tx-id (reduce (tx-handler subscriber)
                                                       after-tx-id
                                                       (if (and latest-submitted-tx-id
                                                                (or (nil? after-tx-id)
                                                                    (< ^long after-tx-id ^long latest-submitted-tx-id)))
                                                         ;; catching up
                                                         (->> (.readRecords log after-tx-id 100)
                                                              (take-while #(<= ^long (.tx-id ^TransactionInstant (.tx ^LogRecord %))
                                                                               ^long latest-submitted-tx-id)))

                                                         ;; running live
                                                         (let [permits (do
                                                                         (.acquire semaphore)
                                                                         (inc (.drainPermits semaphore)))]
                                                           (.readRecords log after-tx-id
                                                                         (if (> permits 100)
                                                                           (do
                                                                             (.release semaphore (- permits 100))
                                                                             100)
                                                                           permits)))))]
                                (when-not (Thread/interrupted)
                                  (recur last-tx-id))))

                            (catch InterruptedException _)

                            (finally
                              (swap! !state update :semaphores disj semaphore)))))
        (.start)))))

(defn ->notifying-subscriber-handler [latest-submitted-tx-id]
  (->NotifyingSubscriberHandler (atom {:latest-submitted-tx-id latest-submitted-tx-id
                                       :semaphores #{}})))
