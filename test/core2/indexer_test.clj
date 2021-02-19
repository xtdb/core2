(ns core2.indexer-test
  (:require [cheshire.core :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.core :as c2]
            [core2.json :as c2-json]
            [core2.metadata :as meta]
            [core2.ts-devices :as ts]
            [core2.tx :as tx]
            [core2.types :as types]
            [core2.util :as util]
            [core2.select :as sel])
  (:import clojure.lang.MapEntry
           [core2.buffer_pool BufferPool MemoryMappedBufferPool]
           [core2.core IngestLoop Node]
           core2.indexer.Indexer
           core2.metadata.IMetadataManager
           [core2.object_store FileSystemObjectStore ObjectStore]
           core2.tx.TransactionInstant
           [java.nio.file Files Path]
           [java.time Clock Duration ZoneId]
           java.util.concurrent.CompletableFuture
           java.util.Date
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector VarCharVector VectorLoader VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector]
           org.apache.arrow.vector.types.Types$MinorType))

(defn- ->mock-clock ^java.time.Clock [^Iterable dates]
  (let [times-iterator (.iterator dates)]
    (proxy [Clock] []
      (getZone []
        (ZoneId/of "UTC"))
      (instant []
        (if (.hasNext times-iterator)
          (.toInstant ^Date (.next times-iterator))
          (throw (IllegalStateException. "out of time")))))))

(defn check-json [^Path expected-path, ^FileSystemObjectStore os]
  (let [^Path os-path (.root-path os)]

    (let [expected-file-count (.count (Files/list expected-path))]
      (t/is (= expected-file-count (.count (Files/list os-path))))
      (t/is (= expected-file-count (count @(.listObjects os)))))

    (c2-json/write-arrow-json-files (.toFile os-path))

    (doseq [^Path path (iterator-seq (.iterator (Files/list os-path)))
            :when (.endsWith (str path) ".json")]
      (t/is (= (json/parse-string (Files/readString (.resolve expected-path (.getFileName path))))
               (json/parse-string (Files/readString path)))
            (str path)))))

(def txs
  [[{:op :put
     :doc {:_id "device-info-demo000000",
           :api-version "23",
           :manufacturer "iobeam",
           :model "pinto",
           :os-name "6.0.1"}}
    {:op :put
     :doc {:_id "reading-demo000000",
           :device-id "device-info-demo000000",
           :cpu-avg-15min 8.654,
           :rssi -50.0,
           :cpu-avg-5min 10.802,
           :battery-status "discharging",
           :ssid "demo-net",
           :time #inst "2016-11-15T12:00:00.000-00:00",
           :battery-level 59.0,
           :bssid "01:02:03:04:05:06",
           :battery-temperature 89.5,
           :cpu-avg-1min 24.81,
           :mem-free 4.10011078E8,
           :mem-used 5.89988922E8}}]
   [{:op :put
     :doc {:_id "device-info-demo000001",
           :api-version "23",
           :manufacturer "iobeam",
           :model "mustang",
           :os-name "6.0.1"}}
    {:op :put
     :doc {:_id "reading-demo000001",
           :device-id "device-info-demo000001",
           :cpu-avg-15min 8.822,
           :rssi -61.0,
           :cpu-avg-5min 8.106,
           :battery-status "discharging",
           :ssid "stealth-net",
           :time #inst "2016-11-15T12:00:00.000-00:00",
           :battery-level 86.0,
           :bssid "A0:B1:C5:D2:E0:F3",
           :battery-temperature 93.7,
           :cpu-avg-1min 4.93,
           :mem-free 7.20742332E8,
           :mem-used 2.79257668E8}}]])

(t/deftest can-build-chunk-as-arrow-ipc-file-format
  (let [node-dir (util/->path "target/can-build-chunk-as-arrow-ipc-file-format")
        mock-clock (->mock-clock [#inst "2020-01-01" #inst "2020-01-02"])
        last-tx-instant (tx/->TransactionInstant 5504 #inst "2020-01-02")
        total-number-of-ops (count (for [tx-ops txs
                                         op tx-ops]
                                     op))]
    (util/delete-dir node-dir)

    (with-open [node (c2/->local-node node-dir)
                tx-producer (c2/->local-tx-producer node-dir {:clock mock-clock})]
      (let [^BufferAllocator a (.allocator node)
            ^ObjectStore os (.object-store node)
            ^Indexer i (.indexer node)
            ^IngestLoop il (.ingest-loop node)
            ^BufferPool bp (.buffer-pool node)
            ^IMetadataManager mm (.metadata-manager node)]

        (t/is (every? nil? @(meta/with-latest-metadata mm
                              (juxt meta/latest-tx meta/latest-row-id))))

        (t/is (= last-tx-instant
                 (last (for [tx-ops txs]
                         @(.submitTx tx-producer tx-ops)))))

        (t/is (= last-tx-instant

                 (.awaitTx il last-tx-instant (Duration/ofSeconds 2))))

        (t/testing "watermark"
          (with-open [watermark (.getWatermark i)]
            (let [column->root (.column->root watermark)
                  first-column (first column->root)
                  last-column (last column->root)]
              (t/is (zero? (.chunk-idx watermark)))
              (t/is (= 4 (.row-count watermark)))
              (t/is (t/is 20 (count column->root)))
              (t/is (= ["_id" 4]
                       [(key first-column) (.getRowCount ^VectorSchemaRoot (val first-column))]))
              (t/is (= ["time" 2]
                       [(key last-column) (.getRowCount ^VectorSchemaRoot (val last-column))])))))

        (.finishChunk i)

        (with-open [watermark (.getWatermark i)]
          (t/is (= 4 (.chunk-idx watermark)))
          (t/is (zero? (.row-count watermark)))
          (t/is (empty? (.column->root watermark))))

        (t/is (= [last-tx-instant (dec total-number-of-ops)]
                 @(meta/with-latest-metadata mm
                    (juxt meta/latest-tx meta/latest-row-id))))

        (let [objects-list @(.listObjects os "metadata-*")]
          (t/is (= 1 (count objects-list)))
          (t/is (= "metadata-00000000.arrow" (first objects-list))))

        (check-json (.toPath (io/as-file (io/resource "can-build-chunk-as-arrow-ipc-file-format"))) os)

        (t/testing "buffer pool"
          (let [buffer-name "metadata-00000000.arrow"
                ^ArrowBuf buffer @(.getBuffer bp buffer-name)
                footer (util/read-arrow-footer buffer)]
            (t/is (= 1 (count (.buffers ^MemoryMappedBufferPool bp))))
            (t/is (instance? ArrowBuf buffer))
            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (with-open [^ArrowBuf same-buffer @(.getBuffer bp buffer-name)]
              (t/is (identical? buffer same-buffer))
              (t/is (= 3 (.getRefCount (.getReferenceManager ^ArrowBuf buffer)))))

            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (t/is (= meta/metadata-schema (.getSchema footer)))
            (t/is (= 1 (count (.getRecordBatches footer))))
            (with-open [^VectorSchemaRoot metadata-batch (VectorSchemaRoot/create (.getSchema footer) a)
                        record-batch (util/->arrow-record-batch-view (first (.getRecordBatches footer)) buffer)]
              (.load (VectorLoader. metadata-batch) record-batch)
              (t/is (= 40 (.getRowCount metadata-batch)))
              (t/is (= "_id" (str (.getObject (.getVector metadata-batch "column") 0))))
              (t/is (= "_row-id" (str (.getObject (.getVector metadata-batch "field") 0))))
              (t/is (= 0 (.getObject (.getVector metadata-batch "min") 0)))
              (t/is (= 3 (.getObject (.getVector metadata-batch "max") 0)))
              (t/is (= 4 (.getObject (.getVector metadata-batch "count") 0)))

              (let [from (.getVector metadata-batch "count")
                    tp (.getTransferPair from a)]
                (with-open [to (.getTo tp)]
                  (t/is (zero? (.getValueCount to)))
                  (.splitAndTransfer tp 0 20)
                  (t/is  (= (.memoryAddress (.getDataBuffer from))
                            (.memoryAddress (.getDataBuffer to))))
                  (t/is (= 20 (.getValueCount to))))))

            (t/is (= 2 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (.close buffer)
            (t/is (= 1 (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))

            (t/is (.evictBuffer bp buffer-name))
            (t/is (zero? (.getRefCount (.getReferenceManager ^ArrowBuf buffer))))
            (t/is (zero? (.getSize (.getReferenceManager ^ArrowBuf buffer))))
            (t/is (empty? (.buffers ^MemoryMappedBufferPool bp)))))))))

(t/deftest can-handle-dynamic-cols-in-same-block
  (let [node-dir (util/->path "target/can-handle-dynamic-cols-in-same-block")
        mock-clock (->mock-clock [#inst "2020-01-01" #inst "2020-01-02" #inst "2020-01-03"])
        tx-ops [{:op :put, :doc {:_id "foo"}}
                {:op :put, :doc {:_id 24.0}}
                {:op :put, :doc {:_id "bar"}}
                {:op :put, :doc {:_id #inst "2021-01-01"}}
                {:op :put, :doc {:_id 52.0}}
                {:op :put, :doc {:_id #inst "2020-01-01"}}]]
    (util/delete-dir node-dir)

    (with-open [node (c2/->local-node node-dir)
                tx-producer (c2/->local-tx-producer node-dir {:clock mock-clock})]
      (let [^ObjectStore os (.object-store node)
            ^Indexer i (.indexer node)
            ^IngestLoop il (.ingest-loop node)]

        (.awaitTx il @(.submitTx tx-producer tx-ops) (Duration/ofSeconds 2))

        (.finishChunk i)

        (check-json (.toPath (io/as-file (io/resource "can-handle-dynamic-cols-in-same-block"))) os)))))

(t/deftest can-stop-node-without-writing-chunks
  (let [node-dir (util/->path "target/can-stop-node-without-writing-chunks")
        mock-clock (->mock-clock [#inst "2020-01-01" #inst "2020-01-02"])
        last-tx-instant (tx/->TransactionInstant 5504 #inst "2020-01-02")]
    (util/delete-dir node-dir)

    (with-open [node (c2/->local-node node-dir)
                tx-producer (c2/->local-tx-producer node-dir {:clock mock-clock})]
      (let [^IngestLoop il (.ingest-loop node)
            object-dir (.resolve node-dir "objects")]

        (t/is (= last-tx-instant
                 (last (for [tx-ops txs]
                         @(.submitTx tx-producer tx-ops)))))

        (t/is (= last-tx-instant
                 (.awaitTx il last-tx-instant (Duration/ofSeconds 2))))
        (t/is (= last-tx-instant (.latestCompletedTx il)))

        (with-open [node (c2/->local-node node-dir)]
          (let [^IngestLoop il (.ingest-loop node)]
            (t/is (= last-tx-instant
                     (.awaitTx il last-tx-instant (Duration/ofSeconds 2))))
            (t/is (= last-tx-instant (.latestCompletedTx il)))))

        (t/is (zero? (.count (Files/list object-dir))))))))

(t/deftest can-ingest-ts-devices-mini
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini")]
    (util/delete-dir node-dir)

    (with-open [node (c2/->local-node node-dir {:max-rows-per-chunk 3000, :max-rows-per-block 300})
                tx-producer (c2/->local-tx-producer node-dir {})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [^BufferAllocator a (.allocator node)
            ^ObjectStore os (.object-store node)
            ^Indexer i (.indexer node)
            ^IngestLoop il (.ingest-loop node)
            ^BufferPool bp (.buffer-pool node)
            ^IMetadataManager mm (.metadata-manager node)
            device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     {:op :put
                      :doc doc})]

        (t/is (= 11000 (count tx-ops)))

        (t/is (nil? (.latestCompletedTx il)))

        (let [last-tx-instant @(reduce
                                (fn [_acc tx-ops]
                                  (.submitTx tx-producer tx-ops))
                                nil
                                (partition-all 100 tx-ops))]

          (t/is (= last-tx-instant (.awaitTx il last-tx-instant (Duration/ofSeconds 5))))
          (t/is (= last-tx-instant (.latestCompletedTx il)))
          (.finishChunk i)

          (t/is [last-tx-instant (dec (count tx-ops))]
               @(meta/with-latest-metadata mm
                  (juxt meta/latest-tx meta/latest-row-id)))

          (t/is (= 4 (count @(.listObjects os "metadata-*"))))
          (t/is (= 1 (count @(.listObjects os "chunk-*-api-version*"))))
          (t/is (= 4 (count @(.listObjects os "chunk-*-battery-level*")))))))

    (c2-json/write-arrow-json-files (.toFile (.resolve node-dir "objects")))

    (t/testing "blocks are row-id aligned"
      (letfn [(row-id-ranges [^String file-name]
                (let [path (.resolve (.resolve node-dir "objects") file-name)]
                  (for [batch (-> (Files/readString path)
                                  json/parse-string
                                  (get "batches"))
                        :let [data (-> (get batch "columns")
                                       first
                                       (get "DATA"))]]
                    [(Long/parseLong (first data))
                     (Long/parseLong (last data))
                     (count data)])))]

        (t/is (= [[0 299 300] [300 599 300] [600 899 300] [900 1199 300]
                  [1200 1499 300] [1500 1799 300] [1800 2099 300]
                  [2100 2399 300] [2400 2699 300] [2700 2999 300]]
                 (row-id-ranges "chunk-00000000-_tx-id.arrow.json")))

        (t/is (= [[0 298 150] [300 598 150] [600 898 150] [900 1198 150]
                  [1200 1498 150] [1500 1798 150] [1800 1998 100]]
                 (row-id-ranges "chunk-00000000-api-version.arrow.json")))

        (t/is (= [[1 299 150] [301 599 150] [601 899 150] [901 1199 150]
                  [1201 1499 150] [1501 1799 150] [1801 2099 200]
                  [2100 2399 300] [2400 2699 300] [2700 2999 300]]
                 (row-id-ranges "chunk-00000000-battery-level.arrow.json")))

        (t/is (= [[3000 3299 300] [3300 3599 300] [3600 3899 300] [3900 4199 300]
                  [4200 4499 300] [4500 4799 300] [4800 5099 300]
                  [5100 5399 300] [5400 5699 300] [5700 5999 300]]
                 (row-id-ranges "chunk-00000bb8-_tx-id.arrow.json")))

        (t/is (= [[3000 3299 300] [3300 3599 300] [3600 3899 300] [3900 4199 300]
                  [4200 4499 300] [4500 4799 300] [4800 5099 300]
                  [5100 5399 300] [5400 5699 300] [5700 5999 300]]
                 (row-id-ranges "chunk-00000bb8-battery-level.arrow.json")))))))

(t/deftest can-ingest-ts-devices-mini-into-multiple-nodes
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini-into-multiple-nodes")
        opts {:max-rows-per-chunk 1000, :max-rows-per-block 100}]
    (util/delete-dir node-dir)

    (with-open [node-1 (c2/->local-node node-dir opts)
                node-2 (c2/->local-node node-dir opts)
                node-3 (c2/->local-node node-dir opts)
                tx-producer (c2/->local-tx-producer node-dir {})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     {:op :put
                      :doc doc})]

        (t/is (= 11000 (count tx-ops)))

        (let [last-tx-instant @(reduce
                                (fn [_ tx-ops]
                                  (.submitTx tx-producer tx-ops))
                                nil
                                (partition-all 100 tx-ops))]

          (doseq [^Node node (shuffle (take 6 (cycle [node-1 node-2 node-3])))
                  :let [il ^IngestLoop (.ingest-loop node)
                        os ^ObjectStore (.object-store node)]]
            (t/is (= last-tx-instant (.awaitTx il last-tx-instant (Duration/ofSeconds 5))))
            (t/is (= last-tx-instant (.latestCompletedTx il)))

            (t/is (= 11 (count @(.listObjects os "metadata-*"))))
            (t/is (= 2 (count @(.listObjects os "chunk-*-api-version*"))))
            (t/is (= 11 (count @(.listObjects os "chunk-*-battery-level*"))))))))))

(t/deftest can-ingest-ts-devices-mini-with-stop-start-and-reach-same-state
  (let [node-dir (util/->path "target/can-ingest-ts-devices-mini-with-stop-start-and-reach-same-state")
        opts {:max-rows-per-chunk 1000, :max-rows-per-block 100}]
    (util/delete-dir node-dir)

    (with-open [tx-producer (c2/->local-tx-producer node-dir {})
                info-reader (io/reader (io/resource "devices_mini_device_info.csv"))
                readings-reader (io/reader (io/resource "devices_mini_readings.csv"))]
      (let [device-infos (map ts/device-info-csv->doc (csv/read-csv info-reader))
            readings (map ts/readings-csv->doc (csv/read-csv readings-reader))
            [initial-readings rest-readings] (split-at (count device-infos) readings)
            tx-ops (for [doc (concat (interleave device-infos initial-readings) rest-readings)]
                     {:op :put
                      :doc doc})
            [first-half-tx-ops second-half-tx-ops] (split-at (/ (count tx-ops) 2) tx-ops)]

        (t/is (= 5500 (count first-half-tx-ops)))
        (t/is (= 5500 (count second-half-tx-ops)))

        (let [^TransactionInstant
              first-half-tx-instant @(reduce
                                      (fn [_ tx-ops]
                                        (.submitTx tx-producer tx-ops))
                                      nil
                                      (partition-all 100 first-half-tx-ops))]

          (with-open [node (c2/->local-node node-dir opts)]
            (let [^Indexer i (.indexer node)
                  ^IngestLoop il (.ingest-loop node)
                  ^ObjectStore os (.object-store node)
                  ^IMetadataManager mm (.metadata-manager node)]
              (t/is (= first-half-tx-instant (.awaitTx il first-half-tx-instant (Duration/ofSeconds 5))))
              (t/is (= first-half-tx-instant (.latestCompletedTx il)))

              (let [[^TransactionInstant os-tx-instant os-latest-row-id] @(meta/with-latest-metadata mm
                                                                            (juxt meta/latest-tx meta/latest-row-id))]
                (t/is (< (.tx-id os-tx-instant) (.tx-id first-half-tx-instant)))
                (t/is (< os-latest-row-id (count first-half-tx-ops)))

                (t/is (= 5 (count @(.listObjects os "metadata-*"))))
                (t/is (= 2 (count @(.listObjects os "chunk-*-api-version*"))))
                (t/is (= 5 (count @(.listObjects os "chunk-*-battery-level*")))))

              (let [^TransactionInstant
                    second-half-tx-instant @(reduce
                                             (fn [_ tx-ops]
                                               (.submitTx tx-producer tx-ops))
                                             nil
                                             (partition-all 100 second-half-tx-ops))]

                (t/is (<= (.tx-id first-half-tx-instant)
                          (.tx-id (.latestCompletedTx il))
                          (.tx-id second-half-tx-instant)))

                (with-open [new-node (c2/->local-node node-dir opts)]
                  (doseq [^Node node [new-node node]
                          :let [^IngestLoop il (.ingest-loop node)]]
                    (t/is (<= (.tx-id first-half-tx-instant)
                              (.tx-id (.latestCompletedTx il))
                              (.tx-id second-half-tx-instant))))

                  (doseq [^Node node [new-node node]
                          :let [^IngestLoop il (.ingest-loop node)]]
                    (t/is (= second-half-tx-instant (.awaitTx il second-half-tx-instant (Duration/ofSeconds 5))))
                    (t/is (= second-half-tx-instant (.latestCompletedTx il))))

                  (Thread/sleep 1000) ;; for now

                  (doseq [^Node node [new-node node]
                          :let [^ObjectStore os (.object-store node)]]
                    (t/is (= 11 (count @(.listObjects os "metadata-*"))))
                    (t/is (= 2 (count @(.listObjects os "chunk-*-api-version*"))))
                    (t/is (= 11 (count @(.listObjects os "chunk-*-battery-level*"))))))))))))))

(t/deftest scans-rowid-for-value
  (let [node-dir (doto (util/->path "target/scans-rowid-for-value")
                   util/delete-dir)]
    (with-open [node (c2/->local-node node-dir)
                tx-producer (c2/->local-tx-producer node-dir)]
      (let [^BufferAllocator allocator (.allocator node)
            ^Indexer i (.indexer node)
            ^IngestLoop il (.ingest-loop node)
            ^BufferPool bp (.buffer-pool node)
            ^IMetadataManager mm (.metadata-manager node)]

        (let [tx @(.submitTx tx-producer [{:op :put, :doc {:name "Håkan", :id 1}}])]

          (.awaitTx il tx (Duration/ofSeconds 2))

          (.finishChunk i))

        (let [last-tx @(.submitTx tx-producer [{:op :put, :doc {:name "James", :id 2}}
                                               {:op :put, :doc {:name "Jon", :id 3}}
                                               {:op :put, :doc {:name "Dan", :id 4}}])]
          (.awaitTx il last-tx (Duration/ofSeconds 2))

          (.finishChunk i))

        (t/testing "where name > 'Ivan'"
          (let [ivan-pred (sel/->str-pred sel/pred> "Ivan")
                matching-chunks (->> (for [chunk-idx (.knownChunks mm)]
                                       (meta/with-metadata mm chunk-idx
                                         (fn [^VectorSchemaRoot metadata-root]
                                           (let [name-idx (meta/field-idx metadata-root "name" "name" Types$MinorType/VARCHAR)]
                                             (when (and (not (neg? name-idx))
                                                        (meta/test-max-value metadata-root name-idx ivan-pred))
                                               chunk-idx)))))
                                     vec
                                     (keep deref))]

            (t/is (= [1] matching-chunks))

            (t/is (= {1 "James", 2 "Jon"}

                     @(letfn [(select-gt-ivans [^VectorSchemaRoot chunk-root]
                                (let [^DenseUnionVector name-vec (.getVector chunk-root "name")
                                      ^BigIntVector row-id-vec (.getVector chunk-root "_row-id")
                                      varchar-type-id (types/arrow-type->type-id (.getType Types$MinorType/VARCHAR))]
                                  (vec (for [idx (-> (sel/select nil name-vec (-> ivan-pred (sel/->dense-union-pred varchar-type-id)))
                                                     .stream .iterator iterator-seq)]
                                         (MapEntry/create (.get row-id-vec idx)
                                                          (-> (.getVectorByType name-vec varchar-type-id)
                                                              (.getObject (.getOffset name-vec idx))
                                                              str))))))]
                        (let [futs (for [chunk-idx matching-chunks]
                                     (-> (.getBuffer bp (meta/->chunk-obj-key chunk-idx "name"))
                                         (util/then-apply
                                           (fn [buffer]
                                             (when buffer
                                               (with-open [^ArrowBuf buffer buffer]
                                                 (->> (util/block-stream buffer allocator)
                                                      (into [] (mapcat select-gt-ivans)))))))))]
                          (-> (CompletableFuture/allOf (into-array CompletableFuture futs))
                              (util/then-apply (fn [_]
                                                 (into {} (mapcat deref) futs))))))))))))))
