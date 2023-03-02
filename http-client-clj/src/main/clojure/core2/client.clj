(ns core2.client
  (:require [cognitect.transit :as transit]
            [core2.api.impl :as api]
            [core2.error :as err]
            [core2.transit :as c2.transit]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.client :as hato]
            [juxt.clojars-mirrors.hato.v0v8v2.hato.middleware :as hato.middleware]
            [juxt.clojars-mirrors.reitit-core.v0v5v15.reitit.core :as r])
  (:import core2.IResultSet
           [java.io EOFException InputStream]
           java.lang.AutoCloseable
           java.util.concurrent.CompletableFuture
           java.util.function.Function
           java.util.NoSuchElementException))

(def transit-opts
  {:decode {:handlers c2.transit/tj-read-handlers}
   :encode {:handlers c2.transit/tj-write-handlers}})

(def router
  (r/router api/http-routes))

(defn- handle-err [e]
  (throw (or (when-let [body (:body (ex-data e))]
               (when (::err/error-type (ex-data body))
                 body))
             e)))

(defn- request
  ([client request-method endpoint]
   (request client request-method endpoint {}))

  ([client request-method endpoint opts]
   (hato/request (merge {:accept :transit+json
                         :as :transit+json
                         :request-method request-method
                         :url (str (:base-url client)
                                   (-> (r/match-by-name router endpoint)
                                       (r/match->path)))
                         :transit-opts transit-opts
                         :async? true}
                        opts)
                 identity handle-err)))

(deftype TransitResultSet [^InputStream in, rdr
                           ^:unsynchronized-mutable next-el]
  IResultSet
  (hasNext [this]
    (or (some? next-el)
        (try
          (set! (.next-el this) (transit/read rdr))
          true
          (catch RuntimeException e
            (if (instance? EOFException (.getCause e))
              false
              (throw e))))))

  (next [this]
    (when-not (.hasNext this)
      (throw (NoSuchElementException.)))
    (let [el (.next-el this)]
      (set! (.next-el this) nil)
      el))

  (close [_]
    (.close in)))

(defmethod hato.middleware/coerce-response-body ::transit+json->resultset [_req {:keys [^InputStream body] :as resp}]
  (try
    (let [rdr (transit/reader body :json {:handlers c2.transit/tj-read-handlers})]
      (-> resp
          (assoc :body (TransitResultSet. body rdr nil))))
    (catch Exception e
      (.close body)
      (throw e))))

(defrecord Core2Client [base-url]
  api/PNode
  (open-datalog& [client query params]
    (let [basis-tx (get-in query [:basis :tx])
          ^CompletableFuture !basis-tx (if (instance? CompletableFuture basis-tx)
                                         basis-tx
                                         (CompletableFuture/completedFuture basis-tx))]
      (-> !basis-tx
          (.thenCompose (reify Function
                          (apply [_ basis-tx]
                            (request client :post :datalog-query
                                     {:content-type :transit+json
                                      :form-params {:query (-> query
                                                               (assoc-in [:basis :tx] basis-tx))
                                                    :params params}
                                      :as ::transit+json->resultset}))))
          (.thenApply (reify Function
                        (apply [_ resp]
                          (:body resp)))))))

  (open-sql& [client query {:keys [basis] :as query-opts}]
    (let [{basis-tx :tx} basis
          ^CompletableFuture !basis-tx (if (instance? CompletableFuture basis-tx)
                                         basis-tx
                                         (CompletableFuture/completedFuture basis-tx))]
      (-> !basis-tx
          (.thenCompose (reify Function
                          (apply [_ basis-tx]
                            (request client :post :sql-query
                                     {:content-type :transit+json
                                      :form-params (into (cond-> {:query query}
                                                           basis-tx (assoc-in [:basis :tx] basis-tx))
                                                         (dissoc query-opts :basis))
                                      :as ::transit+json->resultset}))))
          (.thenApply (reify Function
                        (apply [_ resp]
                          (:body resp)))))))

  api/PSubmitNode
  (submit-tx& [client tx-ops]
    (api/submit-tx& client tx-ops {}))

  (submit-tx& [client tx-ops opts]
    (-> ^CompletableFuture
        (request client :post :tx
                 {:content-type :transit+json
                  :form-params {:tx-ops tx-ops
                                :opts opts}})

        (.thenApply (reify Function
                      (apply [_ resp]
                        (:body resp))))))

  api/PStatus
  (status [client]
    (-> @(request client :get :status)
        :body))

  AutoCloseable
  (close [_]))

(defn start-client ^core2.client.Core2Client [url]
  (->Core2Client url))
