(ns crux.arrow-jnr
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io])
  (:import jnr.ffi.LibraryLoader))

(def ^:const crux-library-path (or (System/getenv "CRUX_LIBRARY_PATH")
                                   (first (for [target ["debug" "release"]
                                                :let [f (io/file (format "../../target/%s/libcrux.so" target))]
                                                :when (.exists f)]
                                            (.getAbsolutePath f)))))

(definterface CruxRs
  (^jnr.ffi.Pointer c_version_string [])
  (^void c_string_free [^jnr.ffi.Pointer c_string]))

(defn -main [& args]
  (let [crux-rs ^CruxRs (.load (LibraryLoader/create CruxRs) crux-library-path)
        c-version-string (.c_version_string crux-rs)]
    (try
      (log/info (.getString c-version-string 0))
      (finally
        (.c_string_free crux-rs c-version-string)))))
