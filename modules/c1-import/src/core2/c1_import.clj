(ns core2.c1-import
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [core2.api :as c2]
            [core2.cli :as cli]
            core2.log
            [core2.node :as node]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import clojure.lang.MapEntry
           java.lang.AutoCloseable
           (java.nio.file ClosedWatchServiceException Files OpenOption Path StandardOpenOption StandardWatchEventKinds WatchEvent WatchEvent$Kind)
           java.util.HashSet))

(defmethod ig/prep-key :core2/c1-import [_ opts]
  (into {:tx-producer (ig/ref :core2.tx-producer/tx-producer)}
        (-> opts
            (update :export-log-path
                    (fn [path]
                      (let [conformed-path (s/conform ::util/path path)]
                        (assert (not (s/invalid? conformed-path))
                                (format "invalid export log path: '%s'" conformed-path))
                        conformed-path))))))

(def ^java.util.concurrent.ThreadFactory thread-factory
  (util/->prefix-thread-factory "c1-import"))

(defn- xform-doc [doc]
  (-> doc
      (set/rename-keys {:crux.db/id :id})
      (->> (into {}
                 (map (fn [[k v]]
                        (MapEntry/create k
                                         (cond-> v
                                           (and (list? v) (= (first v) 'fn))
                                           (c2/->ClojureForm)))))))))

(defn- submit-file! [^core2.tx_producer.TxProducer tx-producer, ^Path log-file]
  (with-open [is (Files/newInputStream log-file (into-array OpenOption #{StandardOpenOption/READ}))]
    (let [rdr (transit/reader is :json)]
      (loop []
        (when-let [[tx-status tx tx-ops] (try
                                           (transit/read rdr)
                                           (catch Throwable _))]
          (when (Thread/interrupted)
            (throw (InterruptedException.)))

          (.submitTx tx-producer
                     (case tx-status
                       :commit (for [[tx-op {:keys [eid doc start-valid-time end-valid-time]}] tx-ops]
                                 ;; HACK: what to do if the user has a separate :id key?
                                 (let [app-time-opts (->> {:app-time-start start-valid-time
                                                           :app-time-end end-valid-time}
                                                          (into {} (filter val)))]
                                   (case tx-op
                                     :put [:put (xform-doc doc) app-time-opts]
                                     :delete [:delete eid app-time-opts]
                                     :evict [:evict eid])))
                       :abort [[:abort]])
                     {:sys-time (:xtdb.api/tx-time tx)})
          (recur))))))

(defmethod ig/init-key :core2/c1-import [_ {:keys [^Path export-log-path tx-producer]}]
  (let [watch-svc (.newWatchService (.getFileSystem export-log-path))
        watch-key (.register export-log-path watch-svc (into-array WatchEvent$Kind [StandardWatchEventKinds/ENTRY_CREATE]))

        t (doto (.newThread thread-factory
                            (fn []
                              (let [seen-files (HashSet.)]
                                (letfn [(process-file! [^Path file]
                                          (when-not (.contains seen-files file)
                                            (.add seen-files file)
                                            (log/infof "processing '%s'..." (str file))
                                            (submit-file! tx-producer file)
                                            (log/infof "processed '%s'." (str file))))]
                                  (try
                                    (run! process-file!
                                          (->> (.toList (Files/list export-log-path))
                                               (sort-by #(let [[_ idx] (re-matches #"log\.(\d+)\.transit\.json" (str (.getFileName ^Path %)))]
                                                           (Long/parseLong idx)))))
                                    (while true
                                      (doseq [^WatchEvent watch-ev (.pollEvents (.take watch-svc))
                                              :let [^Path created-file (.context watch-ev)]]
                                        (process-file! (.resolve export-log-path created-file)))
                                      (.reset watch-key))
                                    (catch InterruptedException _)
                                    (catch ClosedWatchServiceException _)
                                    (catch Throwable t
                                      (log/error t "fail")))))))
            (.start))]
    (reify AutoCloseable
      (close [_]
        (.close watch-svc)
        (.interrupt t)
        (.join t)))))

(defmethod ig/halt-key! :core2/c1-import [_ c1-importer]
  (util/try-close c1-importer))

;; duplicated from core2.cli
(defn- shutdown-hook-promise []
  (let [main-thread (Thread/currentThread)
        shutdown? (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn []
                                 (let [shutdown-ms 10000]
                                   (deliver shutdown? true)
                                   (shutdown-agents)
                                   (.join main-thread shutdown-ms)
                                   (if (.isAlive main-thread)
                                     (do
                                       (log/warn "could not stop node cleanly after" shutdown-ms "ms, forcing exit")
                                       (.halt (Runtime/getRuntime) 1))

                                     (log/info "Node stopped."))))
                               "core2.shutdown-hook-thread"))
    shutdown?))

(defn -main [& args]
  (util/install-uncaught-exception-handler!)

  (let [{::cli/keys [errors help], sys-opts ::cli/node-opts} (cli/parse-args args)]
    (cond
      errors (binding [*out* *err*]
               (doseq [error errors]
                 (println error))
               (System/exit 1))

      help (println help)

      :else (with-open [_submit-node (node/start-submit-node sys-opts)]
              (log/info "Importer started")
              @(shutdown-hook-promise)))

    (shutdown-agents)))

(comment
  (require '[core2.api :as c2])

  (with-open [node (node/start-node {:core2/c1-import {:export-log-path "/tmp/tpch"}})]
    (try
      (c2/datalog-query node (-> '{:find [?cust ?nkey ?n_name]
                                   :where [[?cust :c_nationkey ?nkey]
                                           [?nkey :n_name ?n_name]]
                                   :limit 10}
                                 (assoc :basis {:tx #c2/tx-key {:tx-id 90}})))
      (catch InterruptedException _))))
