(ns core2.core
  (:require [clojure.pprint :as pp]
            [core2.datalog :as d]
            [core2.indexer :as idx]
            core2.ingest-loop
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.tx-producer :as txp]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.indexer.TransactionIndexer
           core2.snapshot.ISnapshotFactory
           core2.tx_producer.ITxProducer
           [java.io Closeable Writer]
           java.lang.AutoCloseable
           java.time.Duration
           [java.util.concurrent CompletableFuture ExecutionException TimeUnit]
           java.util.Date
           [org.apache.arrow.memory BufferAllocator RootAllocator]))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol PClient
  (latest-completed-tx ^core2.tx.TransactionInstant [node])

  ;; we may want to go to `open-query-async`/`Stream` instead, when we have a Java API
  (-plan-query-async ^java.util.concurrent.CompletableFuture [node query basis params]))

(defprotocol PNode
  ;; TODO in theory we shouldn't need this, but it's still used in tests
  (await-tx-async
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [node tx]))

(defprotocol PSubmitNode
  (submit-tx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [tx-producer tx-ops]))

(defrecord Node [^TransactionIndexer indexer
                 ^ISnapshotFactory snapshot-factory
                 ^ITxProducer tx-producer
                 !system
                 close-fn]
  PClient
  (latest-completed-tx [_] (.latestCompletedTx indexer))

  (-plan-query-async [_ query basis params]
    (let [{:keys [default-valid-time basis-tx ^Duration basis-timeout],
           :or {default-valid-time (Date.)}} basis]
      (-> (snap/snapshot-async snapshot-factory basis-tx)
          (cond-> basis-timeout (.orTimeout (.toMillis basis-timeout) TimeUnit/MILLISECONDS))
          (util/then-apply
            (fn [db]
              (let [{:keys [query srcs]} (d/compile-query query params)]
                (op/plan-ra query (merge srcs {'$ db}) {:default-valid-time default-valid-time})))))))

  PNode
  (await-tx-async [_ tx]
    (-> (if-not (instance? CompletableFuture tx)
          (CompletableFuture/completedFuture tx)
          tx)
        (util/then-compose (fn [tx]
                             (.awaitTxAsync indexer tx)))))

  PSubmitNode
  (submit-tx [_ tx-ops]
    (.submitTx tx-producer tx-ops))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defn ^java.util.concurrent.CompletableFuture plan-query-async [node query & params]
  (let [[basis params] (let [[maybe-basis & more-params] params]
                         (if (map? maybe-basis)
                           [maybe-basis more-params]
                           [{} params]))]
    (-plan-query-async node query basis params)))

(defn plan-query [node query & params]
  (try
    @(apply plan-query-async node query params)
    (catch ExecutionException e
      (throw (.getCause e)))))

(let [arglists '([node query & params]
                 [node query basis-opts & params])]
  (alter-meta! #'plan-query-async assoc :arglists arglists)
  (alter-meta! #'plan-query assoc :arglists arglists))

(defmethod print-method Node [_node ^Writer w] (.write w "#<Core2Node>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/prep-key :core2/node [_ opts]
  (merge {:indexer (ig/ref ::idx/indexer)
          :snapshot-factory (ig/ref ::snap/snapshot-factory)
          :tx-producer (ig/ref ::txp/tx-producer)}
         opts))

(defmethod ig/init-key :core2/node [_ deps]
  (map->Node (assoc deps :!system (atom nil))))

(defmethod ig/halt-key! :core2/node [_ ^Node node]
  (.close node))

(defmethod ig/init-key :core2/allocator [_ _] (RootAllocator.))
(defmethod ig/halt-key! :core2/allocator [_ ^BufferAllocator a] (.close a))

(defn- with-default-impl [opts parent-k impl-k]
  (cond-> opts
    (not (ig/find-derived opts parent-k)) (assoc impl-k {})))

(defn start-node ^core2.core.Node [opts]
  (let [system (-> (into {:core2/node {}
                          :core2/allocator {}
                          ::idx/indexer {}
                          :core2.ingest-loop/ingest-loop {}
                          :core2.metadata/metadata-manager {}
                          :core2.temporal/temporal-manager {}
                          :core2.buffer-pool/buffer-pool {}
                          ::snap/snapshot-factory {}
                          ::txp/tx-producer {}}
                         opts)
                   (with-default-impl :core2/log :core2.log/memory-log)
                   (with-default-impl :core2/object-store :core2.object-store/memory-object-store)
                   (doto ig/load-namespaces)
                   ig/prep
                   ig/init)]

    (-> (:core2/node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(do (ig/halt! system)
                              #_(println (.toVerboseString ^RootAllocator (:core2/allocator system))))))))

(defrecord SubmitNode [^ITxProducer tx-producer, !system, close-fn]
  PSubmitNode
  (submit-tx [_ tx-ops]
    (.submitTx tx-producer tx-ops))

  AutoCloseable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method SubmitNode [_node ^Writer w] (.write w "#<Core2SubmitNode>"))
(defmethod pp/simple-dispatch SubmitNode [it] (print-method it *out*))

(defmethod ig/prep-key :core2/submit-node [_ opts]
  (merge {:tx-producer (ig/ref :core2.tx-producer/tx-producer)}
         opts))

(defmethod ig/init-key :core2/submit-node [_ {:keys [tx-producer]}]
  (map->SubmitNode {:tx-producer tx-producer, :!system (atom nil)}))

(defmethod ig/halt-key! :core2/submit-node [_ ^SubmitNode node]
  (.close node))

(defn start-submit-node ^core2.core.SubmitNode [opts]
  (let [system (-> (into {:core2/submit-node {}
                          :core2.tx-producer/tx-producer {}
                          :core2/allocator {}}
                         opts)
                   ig/prep
                   (doto ig/load-namespaces)
                   ig/init)]

    (-> (:core2/submit-node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(ig/halt! system)))))
