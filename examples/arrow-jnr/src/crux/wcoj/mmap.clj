(ns crux.wcoj.mmap
  (:require [clojure.java.io :as io]
            [crux.wcoj.object-store :as wcoj-os])
  (:import java.io.FileInputStream
           java.net.URL
           [java.nio ByteBuffer MappedByteBuffer]
           [java.nio.channels FileChannel$MapMode SeekableByteChannel]
           [java.util HashMap LinkedHashMap Map Map$Entry]
           io.netty.util.internal.PlatformDependent))

(defn new-byte-buffer-seekable-byte-channel ^java.nio.channels.SeekableByteChannel [^ByteBuffer buffer]
  (let [buffer (.slice buffer 0 (.capacity buffer))]
    (proxy [SeekableByteChannel] []
      (isOpen []
        true)

      (close [])

      (read [^ByteBuffer dst]
        (let [src (-> buffer (.slice) (.limit (.remaining dst)))]
          (.put dst src)
          (let [bytes-read (.position src)]
            (.position buffer (+ (.position buffer) bytes-read))
            bytes-read)))

      (position
        ([]
         (.position buffer))
        ([^long new-position]
         (.position buffer new-position)
         this))

      (size []
        (.capacity buffer))

      (write [src]
        (throw (UnsupportedOperationException.)))

      (truncate [size]
        (throw (UnsupportedOperationException.))))))

(defn mmap-file ^java.nio.MappedByteBuffer [f]
  (with-open [in (.getChannel (FileInputStream. (io/file f)))]
    (.map in FileChannel$MapMode/READ_ONLY 0 (.size in))))

(defn unmap-buffer [^MappedByteBuffer buffer]
  (PlatformDependent/freeDirectBuffer buffer))

(defrecord MmapPool [^Map buffer-cache ^Map url-cache object-store])

(defn new-mmap-pool [object-store size]
  (let [buffer-cache (HashMap.)]
    (->MmapPool buffer-cache
                (proxy [LinkedHashMap] [size 0.75 true]
                  (removeEldestEntry [^Map$Entry entry]
                    (if (> (count this) size)
                      (do (unmap-buffer (.remove ^Map buffer-cache (.getValue entry)))
                          true)
                      false)))
                object-store)))

(defn- get-file-url ^java.net.URL [^MmapPool mmap-pool k]
  (when-let [url ^URL (wcoj-os/get-object (.object-store mmap-pool) k)]
    (if-not (= "file" (.getProtocol url))
      (throw (IllegalArgumentException. (str "Not a file:" url)))
      url)))

(defn- mmap-file-url ^java.nio.MappedByteBuffer [^MmapPool mmap-pool url]
  (doto (mmap-file (io/file url))
    (->> (.put ^Map (.buffer-cache mmap-pool) url))))

(defn mmap-object ^java.nio.MappedByteBuffer [^MmapPool mmap-pool k]
  (if-let [url (.get ^Map (.url-cache mmap-pool) k)]
    (or (.get ^Map (.buffer-cache mmap-pool) url)
        (mmap-file-url mmap-pool url))
    (when-let [url (get-file-url mmap-pool k)]
      (.put ^Map (.url-cache mmap-pool) k url)
      (mmap-file-url mmap-pool url))))
