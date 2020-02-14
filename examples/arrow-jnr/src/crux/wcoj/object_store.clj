(ns crux.wcoj.object-store
  (:require [clojure.java.io :as io]
            [crux.wcoj :as wcoj])
  (:import java.io.File
           java.lang.AutoCloseable))

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
    (for [^File f (file-seq dir)
          :when (.isFile f)]
      (io/as-url f)))

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
