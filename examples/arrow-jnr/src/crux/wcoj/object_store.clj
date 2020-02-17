(ns crux.wcoj.object-store
  (:require [clojure.java.io :as io]
            [crux.wcoj :as wcoj])
  (:import java.io.File
           [java.nio.file Files FileVisitResult SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute
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
      (.delete f)
      (when-let [parent (.getParentFile f)]
        (when (empty? (.listFiles parent))
          (.delete parent))))
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

(defn- dir-size ^long [dir]
  (loop [[^File f & fs] (.listFiles (io/file dir))
         size 0]
    (if f
      (recur fs (+ size (if (.isDirectory f)
                          (dir-size f)
                          (.length f))))
      size)))

(defn- create-tmpdir ^java.io.File [dir-name]
  (.toFile (Files/createTempDirectory dir-name (make-array FileAttribute 0))))

(declare evict-object)

(deftype CachedObjectStore [^LocalDirectoryObjectStore object-store-cache object-store]
  ObjectStore
  (get-object [this k]
    (or (get-object object-store-cache k)
        (when-let [v (get-object object-store k)]
          (put-object object-store-cache k v))))

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

(defn- evict-object [^CachedObjectStore cached-object-store k]
  (delete-object (.object-store-cache cached-object-store) k))

(defn new-cached-object-store
  (^crux.wcoj.object_store.CachedObjectStore [object-store]
   (new-cached-object-store (create-tmpdir "cached_object_store") object-store))
  (^crux.wcoj.object_store.CachedObjectStore [cache-dir object-store]
   (->CachedObjectStore (new-local-directory-object-store cache-dir) object-store)))
