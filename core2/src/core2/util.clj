(ns core2.util
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import java.io.ByteArrayOutputStream
           java.lang.AutoCloseable
           [java.lang.invoke LambdaMetafactory MethodHandles MethodType]
           [java.lang.reflect Field Method]
           java.nio.ByteBuffer
           [java.nio.channels Channels FileChannel FileChannel$MapMode SeekableByteChannel]
           java.nio.charset.StandardCharsets
           [java.nio.file Files FileVisitResult LinkOption OpenOption Path SimpleFileVisitor StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.time LocalDateTime ZoneId]
           [java.util ArrayList Date LinkedList List Queue]
           [java.util.concurrent CompletableFuture Executors ExecutorService ThreadFactory TimeUnit]
           java.util.concurrent.atomic.AtomicInteger
           [java.util.function BiFunction Function IntConsumer IntUnaryOperator Supplier]
           java.util.stream.IntStream
           [org.apache.arrow.flatbuf Footer Message RecordBatch]
           [org.apache.arrow.memory ArrowBuf BufferAllocator OwnershipTransferResult ReferenceManager]
           [org.apache.arrow.memory.util ArrowBufPointer ByteFunctionHelpers MemoryUtil]
           [org.apache.arrow.vector BitVector ElementAddressableVector ValueVector VectorLoader VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector NonNullableStructVector]
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamWriter ArrowWriter]
           [org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch MessageSerializer]
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

;;; IO

(defn ->seekable-byte-channel ^java.nio.channels.SeekableByteChannel [^ByteBuffer buffer]
  (let [buffer (.slice buffer)]
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

(def write-new-file-opts ^"[Ljava.nio.file.OpenOption;"
  (into-array OpenOption #{StandardOpenOption/CREATE StandardOpenOption/WRITE StandardOpenOption/TRUNCATE_EXISTING}))

(defn ->file-channel
  (^java.nio.channels.FileChannel [^Path path]
   (->file-channel path #{StandardOpenOption/READ}))
  (^java.nio.channels.FileChannel [^Path path options]
   (FileChannel/open path (into-array OpenOption options))))

(defn ->mmap-path ^java.nio.MappedByteBuffer [^Path path]
  (with-open [in (->file-channel path)]
    (.map in FileChannel$MapMode/READ_ONLY 0 (.size in))))

(defn write-buffer-to-path [^ByteBuffer from-buffer ^Path to-path]
  (with-open [file-ch (->file-channel to-path write-new-file-opts)
              buf-ch (->seekable-byte-channel from-buffer)]
    (.transferFrom file-ch buf-ch 0 (.size buf-ch))))

(def ^:private file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn path-exists [^Path path]
  (Files/exists path (make-array LinkOption 0)))

(defn delete-dir [^Path dir]
  (when (path-exists dir)
    (Files/walkFileTree dir file-deletion-visitor)))

(defn mkdirs [^Path path]
  (Files/createDirectories path (make-array FileAttribute 0)))

(defn ->path ^Path [^String path]
  (.toPath (io/file path)))

(def ^:private ^ZoneId utc (ZoneId/of "UTC"))

(defn local-date-time->date ^java.util.Date [^LocalDateTime ldt]
  (Date/from (.toInstant (.atZone ldt utc))))

(defn date->local-date-time ^java.time.LocalDateTime [^Date d]
  (LocalDateTime/ofInstant (.toInstant d) utc))

(defn ->supplier {:style/indent :defn} ^java.util.function.Supplier [f]
  (reify Supplier
    (get [_]
      (f))))

(defn ->jfn {:style/indent :defn} ^java.util.function.Function [f]
  (reify Function
    (apply [_ v]
      (f v))))

(defn ->jbifn {:style/indent :defn} ^java.util.function.BiFunction [f]
  (reify BiFunction
    (apply [_ a b]
      (f a b))))

(defn ->sam [f ^Class target-interface]
  (let [caller (MethodHandles/lookup)

        implementing-class (class f)
        [implementing-method] (.getDeclaredMethods implementing-class)
        implementing-method-handle (.unreflect caller implementing-method)

        [^Method sam-method] (for [^Method m (.getMethods target-interface)
                                   :when (not (.isDefault m))]
                               m)
        sam-method-type (.type (.unreflect caller sam-method))
        sam-method-type-without-this (.dropParameterTypes sam-method-type 0 1)

        factory-method-type (MethodType/methodType target-interface implementing-class)
        call-site (LambdaMetafactory/metafactory caller
                                                 (.getName sam-method)
                                                 factory-method-type
                                                 sam-method-type-without-this
                                                 implementing-method-handle
                                                 sam-method-type-without-this)
        ^List args [f]]
    (.invokeWithArguments (.getTarget call-site) args)))

(defn then-apply {:style/indent :defn}
  ^java.util.concurrent.CompletableFuture
  [^CompletableFuture fut f]
  (.thenApply fut (->jfn f)))

(defn then-compose {:style/indent :defn} [^CompletableFuture fut f]
  (.thenCompose fut (->jfn f)))

(defmacro completable-future {:style/indent 1} [pool & body]
  `(CompletableFuture/supplyAsync (->supplier (fn [] ~@body)) ~pool))

(defn ->prefix-thread-factory ^java.util.concurrent.ThreadFactory [^String prefix]
  (let [default-thread-factory (Executors/defaultThreadFactory)]
    (reify ThreadFactory
      (newThread [_ r]
        (let [t (.newThread default-thread-factory r)]
          (.setName t (str prefix (.getName t)))
          t)))))

(defn try-close [c]
  (try
    (when (instance? AutoCloseable c)
      (.close ^AutoCloseable c))
    (catch Exception e
      (log/warn e "could not close"))))

(def uncaught-exception-handler
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ thread throwable]
      (log/error throwable "Uncaught exception:"))))

(defn install-uncaught-exception-handler! []
  (when-not (Thread/getDefaultUncaughtExceptionHandler)
    (Thread/setDefaultUncaughtExceptionHandler uncaught-exception-handler)))

(defn shutdown-pool
  ([^ExecutorService pool]
   (shutdown-pool pool 60))
  ([^ExecutorService pool ^long timeout-seconds]
   (.shutdownNow pool)
   (when-not (.awaitTermination pool timeout-seconds TimeUnit/SECONDS)
     (log/warn "pool did not terminate" pool))))

;;; Arrow

(defn root-field-count ^long [^VectorSchemaRoot root]
  (.size (.getFields (.getSchema root))))

(defn slice-root
  (^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root ^long start-idx]
   (slice-root root start-idx (- (.getRowCount root) start-idx)))

  (^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root ^long start-idx ^long len]
   (let [num-fields (root-field-count root)
         acc (ArrayList. num-fields)]
     (dotimes [n num-fields]
       (let [field-vec (.getVector root n)]
         (.add acc (.getTo (doto (.getTransferPair field-vec (.getAllocator field-vec))
                             (.splitAndTransfer start-idx len))))))

     (VectorSchemaRoot. acc))))

(def ^:private ^Field dense-union-value-count-field
  (doto (.getDeclaredField DenseUnionVector "valueCount")
    (.setAccessible true)))

;; TODO: can maybe tweak in DenseUnionVector, but that doesn't
;; solve the VSR calling this.
(defn set-value-count [^ValueVector v ^long value-count]
  (let [value-count (int value-count)]
    (cond
      (instance? DenseUnionVector v)
      (.set dense-union-value-count-field v value-count)

      (and (instance? NonNullableStructVector v)
           (zero? (.getNullCount v)))
      (let [^NonNullableStructVector v v]
        (doseq [v (.getChildrenFromFields v)]
          (set-value-count v value-count))
        (set! (.valueCount v) value-count))

      :else
      (.setValueCount v value-count))))

;; NOTE: also updates value count of the vector.
(defn write-type-id ^long [^DenseUnionVector duv, ^long idx ^long type-id]
  ;; type-id :: byte, return :: int, but Clojure doesn't allow it.
  (let [type-id (unchecked-byte type-id)
        sub-vec (.getVectorByType duv type-id)
        offset (.getValueCount sub-vec)
        offset-buffer (.getOffsetBuffer duv)
        offset-idx (* DenseUnionVector/OFFSET_WIDTH idx)
        offset-buffer (if (>= offset-idx (.capacity offset-buffer))
                        (do (.reAlloc duv)
                            (.getOffsetBuffer duv))
                        offset-buffer)]
    (.setTypeId duv idx type-id)
    (.setInt offset-buffer offset-idx offset)
    (set-value-count sub-vec (inc offset))

    (set-value-count duv (inc idx))

    offset))

(def ^:private ^Field vector-schema-root-row-count-field
  (doto (.getDeclaredField VectorSchemaRoot "rowCount")
    (.setAccessible true)))

(defn set-vector-schema-root-row-count [^VectorSchemaRoot root ^long row-count]
  (let [row-count (int row-count)]
    (.set vector-schema-root-row-count-field root row-count)
    (dotimes [n (root-field-count root)]
      (set-value-count (.getVector root n) row-count))))

(defn build-arrow-ipc-byte-buffer ^java.nio.ByteBuffer {:style/indent 2}
  [^VectorSchemaRoot root ipc-type f]

  (with-open [baos (ByteArrayOutputStream.)
              ch (Channels/newChannel baos)
              ^ArrowWriter sw (case ipc-type
                                :file (ArrowFileWriter. root nil ch)
                                :stream (ArrowStreamWriter. root nil ch))]

    (.start sw)

    (f (fn write-batch! []
         (.writeBatch sw)))

    (.end sw)

    (ByteBuffer/wrap (.toByteArray baos))))

(defn root->arrow-ipc-byte-buffer ^java.nio.ByteBuffer [^VectorSchemaRoot root ipc-type]
  (build-arrow-ipc-byte-buffer root ipc-type
    (fn [write-batch!]
      (write-batch!))))

(def ^{:tag 'bytes} arrow-magic (.getBytes "ARROW1" StandardCharsets/UTF_8))

(defn- validate-arrow-magic [^ArrowBuf ipc-file-format-buffer]
  (when-not (zero? (ByteFunctionHelpers/compare ipc-file-format-buffer
                                                (- (.capacity ipc-file-format-buffer) (alength arrow-magic))
                                                (.capacity ipc-file-format-buffer)
                                                arrow-magic
                                                0
                                                (alength arrow-magic)))
    (throw (IllegalArgumentException. "invalid Arrow IPC file format"))))

(defn read-arrow-footer ^org.apache.arrow.vector.ipc.message.ArrowFooter [^ArrowBuf ipc-file-format-buffer]
  (validate-arrow-magic ipc-file-format-buffer)
  (let [footer-size-offset (- (.capacity ipc-file-format-buffer) (+ Integer/BYTES (alength arrow-magic)))
        footer-size (.getInt ipc-file-format-buffer footer-size-offset)
        footer-position (- footer-size-offset footer-size)
        footer-bb (.nioBuffer ipc-file-format-buffer footer-position footer-size)]
    (ArrowFooter. (Footer/getRootAsFooter footer-bb))))

(defn- try-open-reflective-access [^Class from ^Class to]
  (try
    (let [this-module (.getModule from)]
      (when-not (.isNamed this-module)
        (.addOpens (.getModule to) (.getName (.getPackage to)) this-module)))
    (catch Exception e
      (log/warn e "could not open reflective access from" from "to" to))))

(defonce ^:private direct-byte-buffer-access
  (try-open-reflective-access ArrowBuf (class (ByteBuffer/allocateDirect 0))))

(def ^:private try-free-direct-buffer
  (try
    (Class/forName "io.netty.util.internal.PlatformDependent")
    (eval
     '(fn free-direct-buffer [nio-buffer]
        (io.netty.util.internal.PlatformDependent/freeDirectBuffer nio-buffer)))
    (catch ClassNotFoundException e
      (fn free-direct-buffer-nop [_]))))

(defn inc-ref-count
  (^long [^AtomicInteger ref-count]
   (inc-ref-count ref-count 1))
  (^long [^AtomicInteger ref-count ^long increment]
   (.updateAndGet ^AtomicInteger ref-count
                  (reify IntUnaryOperator
                    (applyAsInt [_ x]
                      (if (pos? x)
                        (+ x increment)
                        x))))))

(defn dec-ref-count ^long [^AtomicInteger ref-count]
  (let [new-ref-count (.updateAndGet ^AtomicInteger ref-count
                                     (reify IntUnaryOperator
                                       (applyAsInt [_ x]
                                         (if (pos? x)
                                           (dec x)
                                           -1))))]
    (when (neg? new-ref-count)
      (.set ref-count 0)
      (throw (IllegalStateException. "ref count has gone negative")))
    new-ref-count))

(deftype NioViewReferenceManager [^BufferAllocator allocator ^:volatile-mutable ^ByteBuffer nio-buffer ^AtomicInteger ref-count]
  ReferenceManager
  (deriveBuffer [this source-buffer index length]
    (ArrowBuf. this
               nil
               length
               (+ (.memoryAddress source-buffer) index)))

  (getAccountedSize [this]
    (.getSize this))

  (getAllocator [this]
    allocator)

  (getRefCount [this]
    (.get ref-count))

  (getSize [this]
    (if-let [nio-buffer nio-buffer]
      (.capacity nio-buffer)
      0))

  (release [this]
    (.release this 1))

  (release [this decrement]
    (when-not (pos? decrement)
      (throw (IllegalArgumentException. "decrement must be positive")))
    (loop [n (dec decrement)]
      (let [new-ref-count (dec-ref-count ref-count)]
        (when (zero? new-ref-count)
          (let [nio-buffer nio-buffer]
            (set! (.nio-buffer this) nil)
            (try-free-direct-buffer nio-buffer)))
        (if (zero? n)
          (zero? new-ref-count)
          (recur (dec n))))))

  (retain [this]
    (.retain this 1))

  (retain [this src-buffer allocator]
    (when-not (identical? allocator (.getAllocator this))
      (throw (IllegalStateException. "cannot retain nio buffer in other allocator")))
    (doto (.slice src-buffer)
      (.readerIndex (.readerIndex src-buffer))
      (.writerIndex (.writerIndex src-buffer))
      (-> (.getReferenceManager) (.retain))))

  (retain [this increment]
    (when-not (pos? increment)
      (throw (IllegalArgumentException. "increment must be positive")))
    (let [new-ref-count (inc-ref-count ref-count increment)]
      (when-not (pos? new-ref-count)
        (throw (IllegalStateException. "ref count was at zero")))))

  (transferOwnership [this source-buffer target-allocator]
    (let [source-buffer (.retain this source-buffer target-allocator)]
      (reify OwnershipTransferResult
        (getAllocationFit [this]
          true)

        (getTransferredBuffer [this]
          source-buffer)))))

(defn ->arrow-buf-view ^org.apache.arrow.memory.ArrowBuf [^BufferAllocator allocator ^ByteBuffer nio-buffer]
  (let [nio-buffer (if (.isDirect nio-buffer)
                     nio-buffer
                     (doto (ByteBuffer/allocateDirect (.capacity nio-buffer))
                       (.put nio-buffer)))]
    (ArrowBuf. (->NioViewReferenceManager allocator nio-buffer (AtomicInteger. 1))
               nil
               (.capacity nio-buffer)
               (MemoryUtil/getByteBufferAddress nio-buffer))))

(defn ->arrow-record-batch-view ^org.apache.arrow.vector.ipc.message.ArrowRecordBatch [^ArrowBlock block ^ArrowBuf buffer]
  (let [prefix-size (if (= (.getInt buffer (.getOffset block)) MessageSerializer/IPC_CONTINUATION_TOKEN)
                      8
                      4)
        ^RecordBatch batch (.header (Message/getRootAsMessage
                                     (.nioBuffer buffer
                                                 (+ (.getOffset block) prefix-size)
                                                 (- (.getMetadataLength block) prefix-size)))
                                    (RecordBatch.))
        body-buffer (doto (.slice buffer
                                  (+ (.getOffset block)
                                     (.getMetadataLength block))
                                  (.getBodyLength block))
                      (.retain))]
    (MessageSerializer/deserializeRecordBatch batch body-buffer)))

(gen-interface
 :name core2.util.IChunkCursor
 :extends [core2.ICursor]
 :methods [[getSchema [] org.apache.arrow.vector.types.pojo.Schema]])

(import core2.util.IChunkCursor)

(deftype ChunkCursor [^ArrowBuf buf,
                      ^Queue blocks
                      ^VectorSchemaRoot root
                      ^VectorLoader loader
                      ^:unsynchronized-mutable ^ArrowRecordBatch current-batch]
  IChunkCursor
  (getSchema [_]
    (.getSchema root))

  (tryAdvance [this c]
    (when current-batch
      (.close current-batch)
      (set! (.current-batch this) nil))

    (if-let [block (.poll blocks)]
      (let [record-batch (->arrow-record-batch-view block buf)]
        (set! (.current-batch this) record-batch)
        (.load loader record-batch)
        (.accept c root)
        true)
      false))

  (close [_]
    (when current-batch
      (.close current-batch))
    (.close root)))

(defn ^core2.util.IChunkCursor ->chunks
  ([^ArrowBuf ipc-file-format-buffer ^BufferAllocator allocator]
   (->chunks ipc-file-format-buffer allocator {}))
  ([^ArrowBuf ipc-file-format-buffer ^BufferAllocator allocator {:keys [^RoaringBitmap block-idxs]}]
   (let [footer (read-arrow-footer ipc-file-format-buffer)
         root (VectorSchemaRoot/create (.getSchema footer) allocator)]
     (ChunkCursor. ipc-file-format-buffer
                   (LinkedList. (cond->> (.getRecordBatches footer)
                                  block-idxs (keep-indexed (fn [idx record-batch]
                                                             (when (.contains block-idxs ^int idx)
                                                               record-batch)))))
                   root
                   (VectorLoader. root)
                   nil))))

(declare du-copy)

(defn project-vec ^org.apache.arrow.vector.ValueVector [^ValueVector in-vec ^IntStream idxs ^long size ^ValueVector out-vec]
  (if (instance? DenseUnionVector in-vec)
    (.forEach idxs (reify IntConsumer
                     (accept [_ idx]
                       (du-copy in-vec idx out-vec (.getValueCount out-vec)))))
    (do (.setInitialCapacity out-vec size)
        (.allocateNew out-vec)
        (.forEach idxs (reify IntConsumer
                         (accept [_ idx]
                           (let [out-idx (.getValueCount out-vec)]
                             (set-value-count out-vec (inc out-idx))
                             (.copyFrom out-vec idx out-idx in-vec)))))))
  out-vec)

(defn maybe-single-child-dense-union ^org.apache.arrow.vector.ValueVector [^ValueVector v]
  (or (when (instance? DenseUnionVector v)
        (let [children-with-elements (for [^ValueVector child (.getChildrenFromFields ^DenseUnionVector v)
                                           :when (pos? (.getValueCount child))]
                                       child)]
          (when (= 1 (count children-with-elements))
            (first children-with-elements))))
      v))

(defn element-addressable-vector? [^ValueVector v]
  (and (instance? ElementAddressableVector v)
       (not (instance? BitVector v))))

(defn vector-at-index ^org.apache.arrow.vector.ValueVector [^ValueVector v ^long idx]
  (if (instance? DenseUnionVector v)
    (let [^DenseUnionVector v v]
      (.getVectorByType v (.getTypeId v idx)))
    v))

(defn pointer-or-object
  ([^ValueVector v ^long idx]
   (pointer-or-object v idx nil))
  ([^ValueVector v ^long idx ^ArrowBufPointer pointer]
   (cond
     (element-addressable-vector? v)
     (.getDataPointer ^ElementAddressableVector v idx (or pointer (ArrowBufPointer.)))

     (instance? DenseUnionVector v)
     (let [v ^DenseUnionVector v]
       (recur (.getVectorByType v (.getTypeId v idx)) (.getOffset v idx) pointer))

     :else
     (.getObject v idx))))

(defn maybe-copy-pointer [^BufferAllocator allocator x]
  (if (instance? ArrowBufPointer x)
    (let [^ArrowBufPointer x x
          length (.getLength x)
          buffer-copy (.buffer allocator length)]
      (.setBytes buffer-copy 0 (.getBuf x) (.getOffset x) length)
      (ArrowBufPointer. buffer-copy 0 length))
    x))

(defn du-copy [^DenseUnionVector src-vec, ^long src-idx, ^DenseUnionVector dest-vec, ^long dest-idx]
  (let [type-id (.getTypeId src-vec src-idx)
        offset (write-type-id dest-vec dest-idx type-id)]
    (.copyFromSafe (.getVectorByType dest-vec type-id)
                   (.getOffset src-vec src-idx)
                   offset
                   (.getVectorByType src-vec type-id))
    nil))

(defn copy-tuple [^VectorSchemaRoot in-root ^long idx ^VectorSchemaRoot out-root ^long out-idx]
  (dotimes [n (root-field-count in-root)]
    (let [in-vec (.getVector in-root n)
          out-vec (.getVector out-root (.getName in-vec))]
      (if (and (instance? DenseUnionVector in-vec)
               (instance? DenseUnionVector out-vec))
        (du-copy in-vec idx out-vec out-idx)
        (.copyFromSafe out-vec idx out-idx in-vec)))))
