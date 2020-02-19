(ns crux.buffer-pool
  (:require [clojure.java.io :as io]
            [crux.object-store :as os])
  (:import java.io.FileInputStream
           java.net.URL
           [java.nio ByteBuffer MappedByteBuffer]
           [java.nio.channels FileChannel$MapMode SeekableByteChannel]
           [java.util HashMap LinkedHashMap Map Map$Entry]
           io.netty.util.internal.PlatformDependent))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol BufferPool
  (get-buffer [this k])
  (evict-buffer [this k]))

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

(defn- get-object-with-file-url ^java.net.URL [object-store k]
  (when-let [url (os/get-object object-store k)]
    (io/file url)
    url))

(defn- mmap-file-from-url ^java.nio.MappedByteBuffer [^Map buffer-cache url]
  (doto (mmap-file (io/file url))
    (->> (.put buffer-cache url))))

(defrecord MmapPool [^Map buffer-cache ^Map url-cache object-store]
  BufferPool
  (get-buffer [this k]
    (if-let [url (.get ^Map url-cache k)]
      (or (.get buffer-cache url)
          (mmap-file-from-url buffer-cache url))
      (when-let [url (get-object-with-file-url object-store k)]
        (.put url-cache k url)
        (mmap-file-from-url buffer-cache url))))

  (evict-buffer [this k]
    (some->> (.remove url-cache k)
             (.remove buffer-cache)
             (unmap-buffer))))

(defn new-mmap-pool [object-store ^long size]
  (let [buffer-cache (HashMap.)]
    (->MmapPool buffer-cache
                (proxy [LinkedHashMap] [size 0.75 true]
                  (removeEldestEntry [^Map$Entry entry]
                    (if (> (count this) size)
                      (do (.remove ^Map buffer-cache (.getValue entry))
                          true)
                      false)))
                object-store)))

(defn- buffer-cache-size ^long [^Map buffer-cache]
  (loop [[b & bs] (vals buffer-cache)
         size 0]
    (if b
      (recur bs (+ size (.capacity ^ByteBuffer b)))
      size)))

(defn free-buffer [^ByteBuffer buffer]
  (PlatformDependent/freeDirectBuffer buffer))

(defrecord InMemoryPool [^Map buffer-cache object-store]
  BufferPool
  (get-buffer [this k]
    (or (some-> ^ByteBuffer (.get buffer-cache k) (.slice))
        (when-let [url (get-object-with-file-url object-store k)]
          (let [f (io/file url)
                buffer (ByteBuffer/allocateDirect (.length f))]
            (with-open [in (.getChannel (FileInputStream. f))]
              (while (pos? (.read in buffer))))
            (let [buffer (.rewind buffer)]
              (.put buffer-cache k buffer)
              (.slice buffer))))))

  (evict-buffer [this k]
    (some->> (.remove buffer-cache k)
             (free-buffer))))

(defn new-in-memory-pool [object-store ^long size-bytes]
  (->InMemoryPool (proxy [LinkedHashMap] [16 0.75 true]
                    (removeEldestEntry [_]
                      (> (buffer-cache-size this) size-bytes)))
                  object-store))
