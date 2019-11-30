(ns crux.arrow-jnr
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io])
  (:import [jnr.ffi LibraryLoader Pointer]
           java.util.List
           [java.nio ByteBuffer ByteOrder]
           [com.google.flatbuffers FlatBufferBuilder FlatBufferBuilder$ByteBufferFactory Table]
           io.netty.buffer.ArrowBuf
           [org.apache.arrow.flatbuf FieldNode Message MessageHeader RecordBatch]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector IntVector VarCharVector VectorSchemaRoot VectorLoader VectorUnloader]
           [org.apache.arrow.vector.ipc ReadChannel WriteChannel]
           [org.apache.arrow.vector.ipc.message ArrowBuffer ArrowFieldNode ArrowMessage ArrowRecordBatch FBSerializables MessageSerializer]
           [org.apache.arrow.vector.types.pojo Schema]))

(def ^:const crux-library-path (or (System/getenv "CRUX_LIBRARY_PATH")
                                   (first (for [target ["debug" "release"]
                                                :let [f (io/file (format "../../target/%s/%s" target (System/mapLibraryName "crux")))]
                                                :when (.exists f)]
                                            (.getAbsolutePath f)))))

(definterface CruxRs
  (^jnr.ffi.Pointer c_version_string [])
  (^void c_string_free [^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} c_string])

  (^void print_schema [^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} schema_fb
                       ^{jnr.ffi.types.size_t true :tag long} schema_len]))

(defn -main [& args]
  (let [crux-rs ^CruxRs (.load (LibraryLoader/create CruxRs) crux-library-path)
        c-version-string (.c_version_string crux-rs)]
    (try
      (log/info (.getString c-version-string 0))
      (finally
        (.c_string_free crux-rs c-version-string)))))

;; https://github.com/tianchen92/jni-poc-java/blob/master/src/main/java/com/odps/arrow/TestArrowJni.java

;; (setenv "RUST_BACKTRACE" "1")

(defonce ^BufferAllocator allocator (RootAllocator. Integer/MAX_VALUE))

(defn- generate-vector-schema-root ^VectorSchemaRoot []
  (let [v1 (doto (IntVector. "v1" allocator)
             (.allocateNewSafe)
             (.setSafe 0 1)
             (.setSafe 1 2)
             (.setSafe 2 3)
             (.setValueCount 3))
        v2 (doto (VarCharVector. "v2" allocator)
             (.allocateNewSafe)
             (.setSafe 0 (.getBytes "aa"))
             (.setSafe 1 (.getBytes "bb"))
             (.setSafe 2 (.getBytes "cc"))
             (.setValueCount 3))
        ^List fields [(.getField v1) (.getField v2)]
        ^List vectors [v1 v2]]
    (VectorSchemaRoot. fields vectors (.getValueCount v1))))

(def ^FlatBufferBuilder$ByteBufferFactory
  off-heap-byte-buffer-factory
  (proxy [FlatBufferBuilder$ByteBufferFactory] []
    (newByteBuffer [capacity]
      (-> (ByteBuffer/allocateDirect capacity)
          (.order ByteOrder/LITTLE_ENDIAN)))))

(defn- serialize-schema ^java.nio.ByteBuffer [^Schema schema]
  (let [builder (FlatBufferBuilder. 1024 off-heap-byte-buffer-factory)
        schema-offset (.getSchema schema builder)]
    (MessageSerializer/serializeMessage builder MessageHeader/Schema schema-offset 0)))

(defn- deserialize-schema ^org.apache.arrow.vector.types.pojo.Schema [^ByteBuffer buffer]
  (MessageSerializer/deserializeSchema (Message/getRootAsMessage buffer)))

(defn- serialize-record-batch ^java.nio.ByteBuffer [^VectorSchemaRoot root]
  (let [builder (FlatBufferBuilder. 1024 off-heap-byte-buffer-factory)
        record-batch (.getRecordBatch (VectorUnloader. root))
        _ (RecordBatch/startNodesVector builder (count (.getNodes record-batch)))
        nodes-offset (FBSerializables/writeAllStructsToVector builder (.getNodes record-batch))
        _ (RecordBatch/startBuffersVector builder (count (.getBuffersLayout record-batch)))
        absolute-buffers (for [[^ArrowBuf buffer ^ArrowBuffer layout] (map vector (.getBuffers record-batch) (.getBuffersLayout record-batch))]
                           (ArrowBuffer. (.memoryAddress buffer) (.getSize layout)))
        buffers-offset (FBSerializables/writeAllStructsToVector builder absolute-buffers)]
    (RecordBatch/startRecordBatch builder)
    (RecordBatch/addLength builder (.getLength record-batch))
    (RecordBatch/addNodes builder nodes-offset)
    (RecordBatch/addBuffers builder buffers-offset)
    (MessageSerializer/serializeMessage builder  MessageHeader/RecordBatch (RecordBatch/endRecordBatch builder) 0)))

(defn- deserialize-message ^org.apache.arrow.flatbuf.Message [^ByteBuffer buffer ^Table table]
  (.header (Message/getRootAsMessage buffer) table))

(defn- deserialize-record-batch ^org.apache.arrow.vector.ipc.message.ArrowRecordBatch [^ByteBuffer buffer]
  (let [^RecordBatch batch (deserialize-message buffer (RecordBatch.))
        no-op-reference-manager (.getReferenceManager (.getEmpty allocator))
        nodes (vec (for [i (range (.nodesLength batch))
                         :let [node (.nodes batch i)]]
                     (ArrowFieldNode. (.length node) (.nullCount node))))
        buffers (vec (for [i (range (.buffersLength batch))
                           :let [buffer (.buffers batch i)]]
                       (ArrowBuf. no-op-reference-manager nil (.length buffer) (.offset buffer) false)))]
    (ArrowRecordBatch. (.length batch) nodes buffers)))

(defn- load-record-batch ^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root ^ByteBuffer buffer]
  (.load (VectorLoader. root) (deserialize-record-batch buffer))
  root)

(defn rust-print-schema [^Schema schema]
  (let [s (serialize-schema schema)
        crux-rs ^CruxRs (.load (LibraryLoader/create CruxRs) crux-library-path)
        crux-rt (jnr.ffi.Runtime/getRuntime crux-rs)
        ptr (Pointer/wrap crux-rt s)]
    (.print_schema crux-rs ptr (.size ptr))))

(comment
  (rust-print-schema (.getSchema (generate-vector-schema-root))))
