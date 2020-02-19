(ns crux.wcoj.object-store
  (:require [clojure.java.io :as io]
            [crux.wcoj :as wcoj])
  (:import java.io.File
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute
           [java.lang.ref Cleaner WeakReference]
           [java.util HashMap LinkedHashMap Map]
           java.lang.AutoCloseable))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol ObjectStore
  (get-object [this k])
  (put-object [this k v])
  (list-objects [this])
  (delete-object [this k]))

(deftype LocalDirectoryObjectStore [^File dir]
  ObjectStore
  (get-object [this k]
    (let [f (io/file dir k)]
      (when (.isFile f)
        (io/as-url f))))

  (put-object [this k v]
    (io/as-url (doto (io/file dir k)
                 (io/make-parents)
                 (->> (io/copy v)))))

  (list-objects [this]
    (let [dir-path (.toPath dir)]
      (for [^File f (file-seq dir)
            :when (.isFile f)]
        (str (.relativize dir-path (.toPath f))))))

  (delete-object [this k]
    (let [f (io/file dir k)]
      (when (.isFile f)
        (.delete f)
        (when-let [parent (.getParentFile f)]
          (when (empty? (.listFiles parent))
            (.delete parent)))))
    nil)

  AutoCloseable
  (close [this]))

(defn new-local-directory-object-store ^crux.wcoj.object_store.LocalDirectoryObjectStore [dir]
  (->LocalDirectoryObjectStore (io/file dir)))

(def ^:private file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn- delete-dir [dir]
  (let [dir (io/file dir)]
    (when (.exists dir)
      (Files/walkFileTree (.toPath dir) file-deletion-visitor))))

(defn- create-tmpdir ^java.io.File [dir-name]
  (.toFile (Files/createTempDirectory dir-name (make-array FileAttribute 0))))

(defonce ^:private ^Cleaner cleaner (Cleaner/create))

(declare evict-object freeze-lru-cache-entries)

(deftype CachedObjectStore [^Map cold-map
                            ^LinkedHashMap lru-cache
                            ^LocalDirectoryObjectStore object-store-cache
                            object-store
                            ^long size-bytes]
  ObjectStore
  (get-object [this k]
    (or (.get lru-cache k)
        (when-let [v-url (or (some-> ^WeakReference (.get cold-map k) (.get))
                             (when-let [source-v-url (get-object object-store k)]
                               (let [v-url (with-open [in (io/input-stream source-v-url)]
                                             (put-object object-store-cache k in))]
                                 (.register cleaner v-url #(when-not (.containsKey lru-cache k)
                                                             (evict-object this k)))
                                 (.put cold-map k (WeakReference. v-url))
                                 v-url)))]
          (.put lru-cache k v-url)
          (.get lru-cache k))))

  (put-object [this k v]
    (evict-object this k)
    (put-object object-store k v))

  (list-objects [this]
    (list-objects object-store))

  (delete-object [this k]
    (evict-object this k)
    (delete-object object-store k))

  AutoCloseable
  (close [this]
    (delete-dir (.dir object-store-cache))))

(def ^:private ^{:tag 'long} freeze-load-factor 0.75)

(defn- evict-object [^CachedObjectStore cached-object-store k]
  (delete-object (.object-store-cache cached-object-store) k)
  (.remove ^Map (.lru-cache cached-object-store) k)
  (.remove ^Map (.cold-map cached-object-store) k))

(defn- lru-cache-file-size ^long [^Map lru-cache]
  (loop [[f & fs] (vals lru-cache)
         size 0]
    (if f
      (recur fs (+ size (.length (io/file f))))
      size)))

(defn new-cached-object-store
  (^crux.wcoj.object_store.CachedObjectStore [object-store ^long size-bytes]
   (new-cached-object-store (create-tmpdir "cached_object_store") object-store size-bytes))
  (^crux.wcoj.object_store.CachedObjectStore [cache-dir object-store ^long size-bytes]
   (->CachedObjectStore (HashMap.)
                        (proxy [LinkedHashMap] [16 0.75 true]
                          (removeEldestEntry [_]
                            (> (lru-cache-file-size this) size-bytes)))
                        (new-local-directory-object-store cache-dir)
                        object-store
                        size-bytes)))
