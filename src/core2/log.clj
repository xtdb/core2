(ns core2.log
  (:require [clojure.tools.logging :as log]
            [core2.tx :as tx]
            [core2.util :as util]
            [core2.system :as sys])
  (:import clojure.lang.MapEntry
           [java.io BufferedInputStream BufferedOutputStream Closeable DataInputStream DataOutputStream EOFException]
           java.nio.ByteBuffer
           [java.nio.channels Channels ClosedByInterruptException FileChannel]
           [java.nio.file Path StandardOpenOption]
           java.time.Clock
           [java.util ArrayList Date]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue CompletableFuture Executors ExecutorService Future]))

(set! *unchecked-math* :warn-on-boxed)

(definterface LogWriter
  (^java.util.concurrent.CompletableFuture appendRecord [^java.nio.ByteBuffer record]))

(definterface LogReader
  (^java.util.List readRecords [^Long after-offset ^int limit]))

(defrecord LogRecord [^long offset ^Date time ^ByteBuffer record])

(defn log-record->tx-instant ^core2.tx.TransactionInstant [^LogRecord record]
  (tx/->TransactionInstant (.offset record) (.time record)))

(def ^:private ^{:tag 'long} header-size (+ Integer/BYTES Integer/BYTES Long/BYTES))

(deftype LocalDirectoryLogWriter [^Path root-path ^ExecutorService pool ^BlockingQueue queue ^Future append-loop-future]
  LogWriter
  (appendRecord [_ record]
    (if (.isShutdown pool)
      (throw (IllegalStateException. "writer is closed"))
      (let [f (CompletableFuture.)]
        (.put queue (MapEntry/create f record))
        f)))

  Closeable
  (close [_]
    (try
      (future-cancel append-loop-future)
      (util/shutdown-pool pool)
      (finally
        (loop []
          (when-let [[^CompletableFuture f] (.poll queue)]
            (when-not (.isDone f)
              (.cancel f true))
            (recur)))))))

(deftype LocalDirectoryLog [^LogWriter log-writer ^Path root-path ^:volatile-mutable ^FileChannel log-channel]
  LogReader
  (readRecords [_ after-offset limit]
    (if (nil? log-channel)
      (let [log-path (.resolve root-path "LOG")]
        (if (util/path-exists log-path)
          (do (set! log-channel (util/->file-channel log-path))
              (recur after-offset limit))
          []))
      (let [log-in (DataInputStream. (BufferedInputStream. (Channels/newInputStream log-channel)))]
        (.position log-channel (long (or after-offset 0)))
        (loop [limit (int (if after-offset
                            (inc limit)
                            limit))
               acc []
               offset (.position log-channel)]
          (if (or (zero? limit) (= offset (.size log-channel)))
            (if after-offset
              (subvec acc 1)
              acc)
            (if-let [record (try
                              (let [check (.readInt log-in)
                                    size (.readInt log-in)
                                    _ (when-not (= check (bit-xor (unchecked-int offset) size))
                                        (throw (IllegalStateException. "invalid record")))
                                    time-ms (.readLong log-in)
                                    record (byte-array size)]
                                (when (= size (.read log-in record))
                                  (->LogRecord offset (Date. time-ms) (ByteBuffer/wrap record))))
                              (catch EOFException _))]
              (recur (dec limit)
                     (conj acc record)
                     (+ offset header-size (.capacity ^ByteBuffer (.record ^LogRecord record))))
              (if after-offset
                (subvec acc 1)
                acc)))))))

  LogWriter
  (appendRecord [_ record]
    (.appendRecord log-writer record))

  Closeable
  (close [_]
    (util/try-close log-writer)
    (when log-channel
      (.close log-channel))))

(defn- writer-append-loop [^Path root-path ^BlockingQueue queue ^Clock clock ^long buffer-size]
  (with-open [log-channel (util/->file-channel (.resolve root-path "LOG")
                                               #{StandardOpenOption/CREATE
                                                 StandardOpenOption/WRITE})]
    (let [elements (ArrayList. buffer-size)]
      (.position log-channel (.size log-channel))
      (while (not (Thread/interrupted))
        (try
          (when-let [element (.take queue)]
            (.add elements element)
            (.drainTo queue elements (.size queue))
            (let [previous-offset (.position log-channel)
                  log-out (DataOutputStream. (BufferedOutputStream. (Channels/newOutputStream log-channel)))]
              (try
                (loop [n (int 0)
                       offset previous-offset]
                  (when-not (= n (.size elements))
                    (let [[f ^ByteBuffer record] (.get elements n)
                          time-ms (.millis clock)
                          size (.remaining record)
                          check (bit-xor (unchecked-int offset) size)
                          written-record (.duplicate record)]
                      (.writeInt log-out check)
                      (.writeInt log-out size)
                      (.writeLong log-out time-ms)
                      (while (>= (.remaining written-record) Long/BYTES)
                        (.writeLong log-out (.getLong written-record)))
                      (while (.hasRemaining written-record)
                        (.write log-out (.get written-record)))
                      (.set elements n (MapEntry/create f (->LogRecord offset (Date. time-ms) record)))
                      (recur (inc n) (+ offset header-size size)))))
                (catch Throwable t
                  (.truncate log-channel previous-offset)
                  (throw t)))
              (.flush log-out)
              (.force log-channel true)
              (doseq [[^CompletableFuture f log-record] elements]
                (.complete f log-record))))
          (catch ClosedByInterruptException e
            (log/warn e "channel interrupted while closing")
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.cancel f true)))
          (catch InterruptedException e
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.cancel f true))
            (.interrupt (Thread/currentThread)))
          (catch Throwable t
            (log/error t "failed appending to log")
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.completeExceptionally f t)))
          (finally
            (.clear elements)))))))

(defn ->local-directory-log-writer {::sys/args {:root-path {:spec ::sys/path, :required? true}
                                                :buffer-size {:spec ::sys/pos-int, :default 4096}
                                                :clock {:spec #(instance? Clock %), :default (Clock/systemUTC)}}}
  [{:keys [^Path root-path buffer-size clock]}]
  (util/mkdirs root-path)
  (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "local-directory-log-writer-"))
        queue (ArrayBlockingQueue. buffer-size)
        append-loop-future (.submit pool ^Runnable #(writer-append-loop root-path queue clock buffer-size))]
    (->LocalDirectoryLogWriter root-path pool queue append-loop-future)))

(defn ->local-directory-log {::sys/args (merge (::sys/args (meta #'->local-directory-log-writer))
                                               {:root-path {:spec ::sys/path, :required? true}})}
  [{:keys [root-path] :as opts}]
  (->LocalDirectoryLog (->local-directory-log-writer opts) root-path nil))
