(ns core2.object-store
  (:require [clojure.string :as str]
            [core2.system :as sys]
            [core2.util :as util])
  (:import java.io.Closeable
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           [java.nio.file CopyOption Files FileSystems Path StandardCopyOption]
           [java.util NavigableMap UUID]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap Executors ExecutorService]))

(set! *unchecked-math* :warn-on-boxed)

(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture #_<ByteBuffer> getObject [^String k]
   "Writes the object to the given path.
    If the object doesn't exist, the CF completes with an IllegalStateException.")

  (^java.util.concurrent.CompletableFuture #_<?> putObject [^String k, ^java.nio.ByteBuffer buf])
  (^java.lang.Iterable #_<String> listObjects [])
  (^java.lang.Iterable #_<String> listObjects [^String prefix])
  (^java.util.concurrent.CompletableFuture #_<?> deleteObject [^String k]))

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))

(deftype InMemoryObjectStore [^NavigableMap os]
  ObjectStore
  (getObject [_this k]
    (CompletableFuture/completedFuture
     (let [^ByteBuffer buf (or (.get os k)
                               (throw (obj-missing-exception k)))]
       (.slice buf))))

  (putObject [_this k buf]
    (.putIfAbsent os k (.slice buf))
    (CompletableFuture/completedFuture nil))

  (listObjects [_this]
    (vec (.keySet os)))

  (listObjects [_this prefix]
    (->> (.keySet (.tailMap os prefix))
         (into [] (take-while #(str/starts-with? % prefix)))))

  (deleteObject [_this k]
    (.remove os k)
    (CompletableFuture/completedFuture nil)))

(defn ->object-store ^core2.object_store.ObjectStore [_]
  (->InMemoryObjectStore (ConcurrentSkipListMap.)))

(deftype FileSystemObjectStore [^Path root-path, ^ExecutorService pool]
  ObjectStore
  (getObject [_this k]
    (CompletableFuture/completedFuture
     (let [from-path (.resolve root-path k)]
       (when-not (util/path-exists from-path)
         (throw (obj-missing-exception k)))

       (util/->mmap-path from-path))))

  (putObject [_this k buf]
    (let [buf (.duplicate buf)]
      (util/completable-future pool
        (let [to-path (.resolve root-path k)]
          (util/mkdirs (.getParent to-path))
          (if (identical? (FileSystems/getDefault) (.getFileSystem to-path))
            (if (util/path-exists to-path)
              to-path

              (let [to-path-temp (.resolveSibling to-path (str "." (UUID/randomUUID)))]
                (try
                  (util/write-buffer-to-path buf to-path-temp)
                  (Files/move to-path-temp to-path (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))
                  (finally
                    (Files/deleteIfExists to-path-temp)))))

            (util/write-buffer-to-path buf to-path))))))

  (listObjects [_this]
    (vec (sort (for [^Path path (iterator-seq (.iterator (Files/list root-path)))]
                 (str (.relativize root-path path))))))

  (listObjects [_this prefix]
    (with-open [dir-stream (Files/newDirectoryStream root-path (str prefix "*"))]
      (vec (sort (for [^Path path dir-stream]
                   (str (.relativize root-path path)))))))

  (deleteObject [_this k]
    (util/completable-future pool
      (Files/deleteIfExists (.resolve root-path k))))

  Closeable
  (close [_this]
    (util/shutdown-pool pool)))

(defn ->file-system-object-store {::sys/args {:root-path {:spec ::sys/path, :required? true}
                                              :pool-size {:spec ::sys/pos-int, :default 4}}}
  [{:keys [root-path pool-size]}]
  (util/mkdirs root-path)
  (let [pool (Executors/newFixedThreadPool pool-size (util/->prefix-thread-factory "file-system-object-store-"))]
    (->FileSystemObjectStore root-path pool)))
