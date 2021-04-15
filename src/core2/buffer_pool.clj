(ns core2.buffer-pool
  (:require core2.object-store
            [core2.system :as sys]
            [core2.util :as util])
  (:import core2.object_store.ObjectStore
           java.io.Closeable
           [java.nio.file Files Path]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]
           java.util.Map
           [org.apache.arrow.memory ArrowBuf BufferAllocator]))

(definterface BufferPool
  (^java.util.concurrent.CompletableFuture getBuffer [^String k])
  (^boolean evictBuffer [^String k]))

(deftype MemoryMappedBufferPool [^Path root-path ^BufferAllocator allocator ^ObjectStore object-store ^Map buffers]
  BufferPool
  (getBuffer [_ k]
    (if (nil? k)
      (CompletableFuture/completedFuture nil)
      (let [v (.getOrDefault buffers k ::not-found)]
        (if-not (= ::not-found v)
          (CompletableFuture/completedFuture (doto ^ArrowBuf v
                                               (.retain)))
          (let [buffer-path (.resolve root-path k)]
            (-> (if (util/path-exists buffer-path)
                  (CompletableFuture/completedFuture buffer-path)
                  (-> (.getObject object-store k)
                      (util/then-apply (fn [buf]
                                         (util/write-buffer-to-path buf buffer-path)
                                         buffer-path))))
                (util/then-apply
                  (fn [^Path buffer-path]
                    (when buffer-path
                      (doto ^ArrowBuf (.computeIfAbsent buffers k (util/->jfn (fn [_]
                                                                                (util/->arrow-buf-view allocator (util/->mmap-path buffer-path)))))
                        (.retain)))))))))))

  (evictBuffer [_ k]
    (when-let [^ArrowBuf buffer (.remove buffers k)]
      (.release buffer)
      (Files/deleteIfExists (.resolve root-path k))))

  Closeable
  (close [_]
    (let [i (.iterator (.values buffers))]
      (while (.hasNext i)
        (.release ^ArrowBuf (.next i))
        (.remove i)))))

(defn ->memory-mapped-buffer-pool {::sys/deps {:allocator :core2/allocator
                                               :object-store :core2/object-store}
                                   ::sys/args {:root-path {:spec ::sys/path, :required? true}}}
  [{:keys [^Path root-path ^BufferAllocator allocator ^ObjectStore object-store]}]
  (util/mkdirs root-path)
  (->MemoryMappedBufferPool root-path allocator object-store (ConcurrentHashMap.)))
