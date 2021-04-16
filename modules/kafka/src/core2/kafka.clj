(ns core2.kafka
  (:require [clojure.java.io :as io]
            [core2.log :as log]
            [core2.system :as sys]
            [core2.util :as util])
  (:import [core2.log LogReader LogWriter]
           java.io.Closeable
           java.nio.file.Path
           java.time.Duration
           [java.util Date Map Properties]
           [java.util.concurrent CompletableFuture ExecutionException]
           [org.apache.kafka.clients.admin AdminClient NewTopic TopicDescription]
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord]
           [org.apache.kafka.common.errors InterruptException TopicExistsException UnknownTopicOrPartitionException]
           org.apache.kafka.common.TopicPartition))

(defn ->kafka-config [{:keys [bootstrap-servers ^Path properties-file properties-map]}]
  (merge {"bootstrap.servers" bootstrap-servers}
         (when properties-file
           (with-open [in (io/reader (.toFile properties-file))]
             (->> (doto (Properties.)
                    (.load in))
                  (into {}))))
         properties-map))

(defn ->topic-opts [opts]
  (-> opts
      (update :topic-config (fn [config]
                              (merge {"message.timestamp.type" "LogAppendTime"}
                                     config)))))

(deftype KafkaLogWriter [^KafkaProducer producer
                         topic-name]
  LogWriter
  (appendRecord [_ record]
    (let [fut (CompletableFuture.)]
      (.send producer (ProducerRecord. topic-name nil record)
             (reify Callback
               (onCompletion [_ record-metadata e]
                 (if e
                   (.completeExceptionally fut e)
                   (.complete fut (log/->LogRecord (.offset record-metadata)
                                                   (Date. (.timestamp record-metadata))
                                                   record))))))
      fut))

  Closeable
  (close [_]
    (.close producer)))

(deftype KafkaLog [^LogWriter log-writer
                   ^KafkaConsumer consumer
                   ^TopicPartition tp
                   ^Duration poll-duration]
  LogReader
  (readRecords [_ after-offset limit]
    (if after-offset
      (.seek consumer tp (inc ^long after-offset))
      (.seekToBeginning consumer [tp]))

    (try
      (->> (for [^ConsumerRecord record (.poll consumer poll-duration)]
             (log/->LogRecord (.offset record)
                              (Date. (.timestamp record))
                              (.value record)))
           (into [] (take limit)))
      (catch InterruptException e
        (throw (.getCause e)))))

  LogWriter
  (appendRecord [_ record]
    (.appendRecord log-writer record))

  Closeable
  (close [_]
    (util/try-close consumer)
    (util/try-close log-writer)))

(defn ->producer [kafka-config]
  (KafkaProducer. ^Map (merge {"enable.idempotence" "true"
                               "acks" "all"
                               "compression.type" "snappy"
                               "key.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"
                               "value.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"}
                              kafka-config)))

(defn ->consumer ^org.apache.kafka.clients.consumer.KafkaConsumer [kafka-config]
  (KafkaConsumer. ^Map (merge {"enable.auto.commit" "false"
                               "isolation.level" "read_committed"
                               "auto.offset.reset" "earliest"
                               "key.deserializer" "org.apache.kafka.common.serialization.ByteBufferDeserializer"
                               "value.deserializer" "org.apache.kafka.common.serialization.ByteBufferDeserializer"}
                              kafka-config)))

(defn ensure-topic-exists [kafka-config {:keys [topic-name replication-factor create-topic? topic-config]}]
  (with-open [admin-client (AdminClient/create ^Map kafka-config)]
    (or (when-let [^TopicDescription
                   desc (-> (try
                              @(.all (.describeTopics admin-client [topic-name]))
                              (catch ExecutionException e
                                (let [e (.getCause e)]
                                  (when-not (instance? UnknownTopicOrPartitionException e)
                                    (throw e)))))
                            (get topic-name))]
          (let [partition-count (count (.partitions desc))]
            (when-not (= 1 partition-count)
              (throw (IllegalStateException. (format "'%s' topic has %d partitions, required 1"
                                                     topic-name partition-count))))
            :exists))

        (when create-topic?
          (let [new-topic (doto (NewTopic. ^String topic-name
                                           1 ; num-partitions
                                           ^short (short replication-factor))
                            (.configs topic-config))]
            (try
              @(.all (.createTopics admin-client [new-topic]))
              :created
              (catch ExecutionException e
                (let [cause (.getCause e)]
                  (when-not (instance? TopicExistsException cause)
                    (throw e)))))))

        (throw (IllegalStateException. (format "Topic '%s' does not exist", topic-name))))))

(def kafka-opt-spec
  {:bootstrap-servers {:spec ::sys/string
                       :doc "URL for connecting to Kafka, eg \"kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092\""
                       :required? true
                       :default "localhost:9092"}
   :properties-file {:spec ::sys/path
                     :doc "Used for supplying Kafka connection properties to the underlying Kafka API."}
   :properties-map {:spec ::sys/string-map
                    :doc "Used for supplying Kafka connection properties to the underlying Kafka API."}
   :topic-name {:required? true, :spec ::sys/string}
   :replication-factor {:required? true
                        :default 1
                        :doc "Level of durability for Kafka"
                        :spec ::sys/pos-int}
   :create-topic? {:required? true
                   :default true
                   :doc "Create topic if it doesn't exist"
                   :spec ::sys/boolean}
   :topic-config {:spec ::sys/string-map}})

(defn ->log-writer {::sys/args kafka-opt-spec}
  [{:keys [topic-name] :as kafka-opts}]
  (let [kafka-config (->kafka-config kafka-opts)]
    (ensure-topic-exists kafka-config kafka-opts)
    (KafkaLogWriter. (->producer kafka-config) topic-name)))

(defn ->log {::sys/args (merge kafka-opt-spec
                               {:poll-duration {:spec ::sys/duration
                                                :required? true
                                                :doc "How long to wait when polling Kafka"
                                                :default (Duration/ofSeconds 1)}})}
  [{:keys [topic-name poll-duration] :as kafka-opts}]
  (let [kafka-config (->kafka-config kafka-opts)
        tp (TopicPartition. topic-name 0)]
    (KafkaLog. (->log-writer kafka-opts)
               (doto (->consumer kafka-config)
                 (.assign #{tp}))
               tp
               poll-duration)))
