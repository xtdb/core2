(ns core2.json
  (:require [clojure.java.io :as io])
  (:import [java.io ByteArrayInputStream File]
           [java.nio.channels Channels FileChannel]
           [java.nio.file OpenOption StandardOpenOption]
           org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.VectorSchemaRoot
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowStreamReader JsonFileWriter]))

(set! *unchecked-math* :warn-on-boxed)

(defn- file->json-file ^java.io.File [^File file]
  (io/file (.getParentFile file) (format "%s.json" (.getName file))))

(defn write-arrow-json-files [^File arrow-dir]
  (with-open [allocator (RootAllocator. Long/MAX_VALUE)]
    (doseq [^File
            file (->> (.listFiles arrow-dir)
                      (filter #(.endsWith (.getName ^File %) ".arrow")))]
      (with-open [file-ch (FileChannel/open (.toPath file)
                                            (into-array OpenOption #{StandardOpenOption/READ}))
                  file-reader (ArrowFileReader. file-ch allocator)
                  file-writer (JsonFileWriter. (file->json-file file)
                                               (.. (JsonFileWriter/config) (pretty true)))]
        (let [root (.getVectorSchemaRoot file-reader)]
          (.start file-writer (.getSchema root) nil)
          (while (.loadNextBatch file-reader)
            (.write file-writer root)))))))

(defn arrow-streaming->json ^String [^bytes bs]
  (let [json-file (File/createTempFile "arrow" "json")]
    (try
      (with-open [allocator (RootAllocator. Long/MAX_VALUE)
                  in-ch (Channels/newChannel (ByteArrayInputStream. bs))
                  file-reader (ArrowStreamReader. in-ch allocator)
                  file-writer (JsonFileWriter. json-file (.. (JsonFileWriter/config) (pretty true)))]
        (let [root (.getVectorSchemaRoot file-reader)]
          (.start file-writer (.getSchema root) nil)
          (while (.loadNextBatch file-reader)
            (.write file-writer root))))
      (slurp json-file)
      (finally
        (.delete json-file)))))

(defn vector-schema-root->json ^String [^VectorSchemaRoot root]
  (let [json-file (File/createTempFile "arrow" "json")]
    (try
      (with-open [file-writer (JsonFileWriter. json-file (.. (JsonFileWriter/config) (pretty true)))]
        (.start file-writer (.getSchema root) nil)
        (.write file-writer root))
      (slurp json-file)
      (finally
        (.delete json-file)))))
