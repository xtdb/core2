(ns core2.util
  (:require [clojure.java.io :as io])
  (:import [org.apache.arrow.vector ValueVector]
           [org.apache.arrow.vector.complex DenseUnionVector]
           org.apache.arrow.flatbuf.Footer
           org.apache.arrow.vector.ipc.message.ArrowFooter
           java.io.File
           [java.nio ByteBuffer ByteOrder]
           [java.nio.channels FileChannel SeekableByteChannel]
           java.nio.charset.StandardCharsets
           [java.nio.file Files FileVisitResult OpenOption StandardOpenOption SimpleFileVisitor Path]
           java.nio.file.attribute.FileAttribute
           java.util.Date
           [java.util.function Supplier Function]
           java.util.concurrent.CompletableFuture
           [java.time LocalDateTime ZoneId]))

(defn ->seekable-byte-channel ^java.nio.channels.SeekableByteChannel [^ByteBuffer buffer]
  (let [buffer (.duplicate buffer)]
    (proxy [SeekableByteChannel] []
      (isOpen []
        true)

      (close [])

      (read [^ByteBuffer dst]
        (let [^ByteBuffer src (-> buffer (.slice) (.limit (.remaining dst)))]
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

(defn open-write-file-ch ^java.nio.channels.FileChannel [^File file]
  (FileChannel/open (.toPath file)
                    (into-array OpenOption #{StandardOpenOption/CREATE
                                             StandardOpenOption/WRITE
                                             StandardOpenOption/TRUNCATE_EXISTING})))

(def ^:private file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn delete-dir [dir]
  (let [dir (io/file dir)]
    (when (.exists dir)
      (Files/walkFileTree (.toPath dir) file-deletion-visitor))))

(defn mkdirs [^Path path]
  (Files/createDirectories path (make-array FileAttribute 0)))

(def ^:private ^ZoneId utc (ZoneId/of "UTC"))

(defn local-date-time->date ^java.util.Date [^LocalDateTime ldt]
  (Date/from (.toInstant (.atZone ldt utc))))

(defn ->supplier {:style/indent :defn} ^java.util.function.Supplier [f]
  (reify Supplier
    (get [_]
      (f))))

(defn ->jfn {:style/indent :defn} ^java.util.function.Function [f]
  (reify Function
    (apply [_ v]
      (f v))))

(defn then-apply {:style/indent :defn} [^CompletableFuture fut f]
  (.thenApply fut (->jfn f)))

(defn then-compose {:style/indent :defn} [^CompletableFuture fut f]
  (.thenCompose fut (->jfn f)))

(defmacro completable-future {:style/indent 1} [pool & body]
  `(CompletableFuture/supplyAsync (->supplier (fn [] ~@body)) ~pool))

(definterface DenseUnionWriter
  (^int writeTypeId [^byte type-id])
  (^void end []))

(deftype DenseUnionWriterImpl [^DenseUnionVector duv
                               ^:unsynchronized-mutable ^int value-count
                               ^ints offsets]
  DenseUnionWriter
  (writeTypeId [this type-id]
    (while (< (.getValueCapacity duv) (inc value-count))
      (.reAlloc duv))

    (let [offset (aget offsets type-id)
          offset-buffer (.getOffsetBuffer duv)]
      (.setTypeId duv value-count type-id)
      (.setInt offset-buffer (* DenseUnionVector/OFFSET_WIDTH value-count) offset)

      (set! (.value-count this) (inc value-count))
      (aset offsets type-id (inc offset))

      offset))

  (end [_]
    (.setValueCount duv value-count)))

(defn ->dense-union-writer ^core2.util.DenseUnionWriter [^DenseUnionVector duv]
  (->DenseUnionWriterImpl duv
                          (.getValueCount duv)
                          (int-array (for [^ValueVector child-vec (.getChildrenFromFields duv)]
                                       (.getValueCount child-vec)))))

(def ^:private ^{:tag 'long} arrow-magic-size (alength (.getBytes "ARROW1" StandardCharsets/UTF_8)))

(defn read-footer-position ^long [^SeekableByteChannel in]
  (let [footer-size-bb (.order (ByteBuffer/allocate Integer/BYTES) ByteOrder/LITTLE_ENDIAN)
        footer-size-offset (- (.size in) (+ (.capacity footer-size-bb) arrow-magic-size))]
    (.position in footer-size-offset)
    (while (pos? (.read in footer-size-bb)))
    (- footer-size-offset (.getInt footer-size-bb 0))))

(defn read-footer ^org.apache.arrow.vector.ipc.message.ArrowFooter [^SeekableByteChannel in]
  (let [footer-position (read-footer-position in)
        footer-size (- (.size in) footer-position)
        bb (ByteBuffer/allocate footer-size)]
    (.position in footer-position)
    (while (pos? (.read in bb)))
    (.flip bb)
    (ArrowFooter. (Footer/getRootAsFooter bb))))
