(ns core2.bench.ts-devices
  (:require [clojure.tools.logging :as log]
            [core2.bench :as bench]
            [core2.core :as c2]
            [core2.ts-devices :as tsd])
  (:import java.util.concurrent.TimeUnit))

(def cli-arg-spec
  [[nil "--size <small|med|big>" "Size of ts-devices files to use"
    :id :size
    :default :small
    :parse-fn keyword
    :validate-fn (comp boolean #{:small :med :big})]])

(defn download-file [size file-name]
  (let [tmp-file (bench/tmp-file-path (str "ts-devices." file-name) ".csv.gz")]
    (bench/download-s3-dataset-file (format "ts-devices/%s/devices_%s_%s.csv.gz"
                                            (name size) (name size) file-name)
                                    tmp-file)
    (.toFile tmp-file)))

(defn ingest-tsd [node {:keys [device-info-file readings-file]}]
  (let [tx (bench/with-timing :submit-docs
             (tsd/submit-ts-devices node {:device-info-file device-info-file
                                          :readings-file readings-file}))]
    (bench/with-timing :await-tx
      @(-> (c2/await-tx-async node tx)
           (.orTimeout 5 TimeUnit/HOURS)))

    (bench/with-timing :finish-chunk
      (bench/finish-chunk node))))

(defn -main [& args]
  (try
    (let [{:keys [size] :as opts} (or (bench/parse-args cli-arg-spec args)
                                      (System/exit 1))]
      (log/info "Opts: " (pr-str opts))
      (let [downloaded-files (bench/with-timing :download-files
                               {:device-info-file (download-file size "device_info")
                                :readings-file (download-file size "readings")})]
        (with-open [node (bench/start-node)]
          (bench/with-timing :ingest
            (ingest-tsd node downloaded-files)))))

    (catch Exception e
      (.printStackTrace e)
      (System/exit 1))

    (finally
      (shutdown-agents))))
