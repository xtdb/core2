(ns core2.s3
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [core2.object-store :as os]
            [core2.system :as sys]
            [core2.util :as util])
  (:import core2.object_store.ObjectStore
           core2.s3.S3Configurator
           java.io.Closeable
           java.util.function.Function
           java.util.concurrent.CompletableFuture
           software.amazon.awssdk.core.async.AsyncRequestBody
           [software.amazon.awssdk.services.s3.model DeleteObjectRequest GetObjectRequest HeadObjectRequest ListObjectsV2Request ListObjectsV2Response NoSuchKeyException PutObjectRequest S3Object]
           software.amazon.awssdk.services.s3.S3AsyncClient))

(defrecord S3ObjectStore [^S3Configurator configurator ^S3AsyncClient client bucket prefix]
  ObjectStore
  (getObject [_ k to-path]
    (-> (.getObject client
                    (-> (GetObjectRequest/builder)
                        (.bucket bucket)
                        (.key (str prefix k))
                        (->> (.configureGet configurator))
                        ^GetObjectRequest (.build))
                    to-path)
        (.exceptionally (reify Function
                          (apply [_ e]
                            (try
                              (throw (.getCause ^Exception e))
                              (catch NoSuchKeyException _
                                (throw (os/obj-missing-exception k)))))))))

  (putObject [_ k buf]
    (-> (.headObject client
                     (-> (HeadObjectRequest/builder)
                         (.bucket bucket)
                         (.key (str prefix k))
                         (->> (.configureHead configurator))
                         ^HeadObjectRequest (.build)))
        (util/then-apply (fn [_resp] true))
        (.exceptionally (reify Function
                          (apply [_ e]
                            (let [e (.getCause ^Exception e)]
                              (if (instance? NoSuchKeyException e)
                                false
                                (throw e))))))
        (util/then-compose (fn [exists?]
                             (if exists?
                               (CompletableFuture/completedFuture nil)
                               (.putObject client
                                           (-> (PutObjectRequest/builder)
                                               (.bucket bucket)
                                               (.key (str prefix k))
                                               (->> (.configurePut configurator))
                                               ^PutObjectRequest (.build))
                                           (AsyncRequestBody/fromByteBuffer buf)))))))

  (listObjects [this] (.listObjects this nil))

  (listObjects [_ obj-prefix]
    (letfn [(list-objects* [continuation-token]
              (lazy-seq
               (let [^ListObjectsV2Request
                     req (-> (ListObjectsV2Request/builder)
                             (.bucket bucket)
                             (.prefix (str prefix obj-prefix))
                             (cond-> continuation-token (.continuationToken continuation-token))
                             (.build))

                     ^ListObjectsV2Response
                     resp (.get (.listObjectsV2 client req))]

                 (concat (for [^S3Object object (.contents resp)]
                           (subs (.key object) (count prefix)))
                         (when (.isTruncated resp)
                           (list-objects* (.nextContinuationToken resp)))))))]
      (list-objects* nil)))

  (deleteObject [_ k]
    (.deleteObject client
                   (-> (DeleteObjectRequest/builder)
                       (.bucket bucket)
                       (.key (str prefix k))
                       ^DeleteObjectRequest (.build))))

  Closeable
  (close [_]
    (.close client)))

(s/def ::bucket string?)
(s/def ::prefix (s/and string?
                       (s/conformer (fn [prefix]
                                      (cond
                                        (string/blank? prefix) ""
                                        (string/ends-with? prefix "/") prefix
                                        :else (str prefix "/"))))))

(defn ->object-store {::sys/args {:bucket {:required? true,
                                           :spec ::bucket
                                           :doc "S3 bucket"}
                                  :prefix {:required? false,
                                           :spec ::prefix
                                           :doc "S3 prefix"}}
                      ::sys/deps {:configurator (fn [_] (reify S3Configurator))}}

  [{:keys [bucket prefix ^S3Configurator configurator]}]
  (->S3ObjectStore configurator
                   (.makeClient configurator)
                   bucket prefix))
