(ns core2.core-test
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.instant :as inst]
            [clojure.string :as str]
            [core2.core :as c2])
  (:import [java.io Closeable File]
           [java.util Date HashMap Map]
           [java.util.function Function]
           [org.apache.arrow.memory RootAllocator]
           [core2.core Ingester]))

(def device-info-csv-resource
  (io/resource "devices_small_device_info.csv"))

(def !info-docs
  (delay
    (when device-info-csv-resource
      (with-open [rdr (io/reader device-info-csv-resource)]
        (vec (for [[device-id api-version manufacturer model os-name] (csv/read-csv rdr)]
               {:_id (str "device-info-" device-id)
                :api-version api-version
                :manufacturer manufacturer
                :model model
                :os-name os-name}))))))

(def readings-csv-resource
  (io/resource "devices_small_readings.csv"))

(defn with-readings-docs [f]
  (when readings-csv-resource
    (with-open [rdr (io/reader readings-csv-resource)]
      (f (for [[time device-id battery-level battery-status
                battery-temperature bssid
                cpu-avg-1min cpu-avg-5min cpu-avg-15min
                mem-free mem-used rssi ssid]
               (csv/read-csv rdr)]
           {:_id (str "reading-" device-id)
            :time (inst/read-instant-date
                   (-> time
                       (str/replace " " "T")
                       (str/replace #"-(\d\d)$" ".000-$1:00")))
            :battery-level (Double/parseDouble battery-level)
            :battery-status battery-status
            :battery-temperature (Double/parseDouble battery-temperature)
            :bssid bssid
            :cpu-avg-1min (Double/parseDouble cpu-avg-1min)
            :cpu-avg-5min (Double/parseDouble cpu-avg-5min)
            :cpu-avg-15min (Double/parseDouble cpu-avg-15min)
            :mem-free (Double/parseDouble mem-free)
            :mem-used (Double/parseDouble mem-used)
            :rssi (Double/parseDouble rssi)
            :ssid ssid})))))

(comment
  (defonce foo-rows
    (with-readings-docs
      (fn [rows]
        (doall (take 10 rows))))))

;;; ingest
;; TODO 4. writing metadata - minmax, bloom at chunk/file and block/record-batch
;; TODO 11. figure out last tx-id/row-id from latest chunk and resume ingest on start.
;; TODO 12. object store protocol, store chunks and metadata. File implementation.
;; TODO 13. log protocol. File implementation.

;;; query
;; TODO 7. reading any blocks - select battery_level from db (simple code-level query)
;; TODO 7b. fetch metadata and find further chunks based on metadata.
;; TODO 8. reading live blocks
;; TODO 8a. reading chunks already written to disk
;; TODO 8b. reading blocks already written to disk
;; TODO 8c. reading current block not yet written to disk
;; TODO 8d. VSR committed read slice

;;; future
;; TODO 3d. dealing with schema that changes throughout an ingest (promotable unions, including nulls)
;; TODO 5. dictionaries
;; TODO 6. consider eviction
;; TODO 1b. writer?
;; TODO 15. handle deletes

;; directions?
;; 1. e2e? submit-tx + some code-level queries
;;    transactions, evictions, timelines, etc.
;; 2. quickly into JMH, experimentation

;; once we've sealed a _chunk_ (/ block?), throw away in-memory? mmap or load
;; abstract over this - we don't need to know whether it's mmap'd or in-memory

;; reading a block before it's sealed?
;; theory: if we're only appending, we should be ok?

;; two different cases:
;; live chunk, reading sealed block (maybe) written to disk
;; live chunk, reading unsealed block in memory

'{:tx-time 'date-milli
  :tx-id 'uint8
  :tx-ops {:put [{:documents []
                  :start-vts [...]
                  :end-vts [...]}]
           :delete []}}

;; submit-tx :: ops -> Arrow bytes
;; Kafka mock - Arrow * tx-time/tx-id
;; ingester :: Arrow bytes * tx-time * tx-id -> indices


#_(def foo-tx-bytes
    (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
      (with-readings-docs
        (fn [readings]
          (let [info-docs @!info-docs]
            (vec (for [tx-batch (->> (concat (interleave info-docs
                                                         (take (count info-docs) readings))
                                             (drop (count info-docs) readings))
                                     (partition-all 1000))]
                   (c2/submit-tx (for [doc tx-batch]
                                   {:op :put, :doc doc})
                                 allocator))))))))

#_(def ^:private metadata-schema
    (Schema. [(->)]))

'{:file-name ["name.arrow" "age.arrow"]
  :field-name ["name" "age"]
  :field-metadata [{:min 4 :max 2300 :count 100}
                   {:min "Aaron" :max "Zach" :count 1000}]
  :row-id-metadata [{:min 4 :max 2300 :count 100}
                    {:min 10 :max 30 :count 1000}]}

'{:metadata [{:chunk_00000_age {:_row-id {:min 4 :max 2300 :count 100}
                                :age {:min 10 :max 30 :count 1000}}
              :chunk_00000_name {:_row-id {:min 4 :max 2300 :count 100}
                                 :name {:min "Aaron" :max "Zach" :count 1000}}}]}

'{:chunk_00000_age [{:_row-id {:min 4 :max 2300 :count 100}
                     :age {:min 10 :max 30 :count 1000}}]
  :chunk_00000_name [{:_row-id {:min 4 :max 2300 :count 100}
                      :name {:min "Aaron" :max "Zach" :count 1000}}]}

;; aim: one metadata file per chunk with:
;; - count per column
;; - min-max per column


(comment
  (let [arrow-dir (doto (io/file "/tmp/arrow")
                    .mkdirs)]
    (with-open [allocator (RootAllocator. Long/MAX_VALUE)
                ingester (c2/->ingester allocator arrow-dir)]

      (doseq [[tx-id tx-bytes] (->> (map-indexed vector foo-tx-bytes)
                                    (take 15))]
        (c2/index-tx ingester {:tx-id tx-id, :tx-time (Date.)} tx-bytes)))

    (c2/write-arrow-json-files arrow-dir)))
