(ns crux.arrow-jnr
  (:require [clojure.tools.logging :as log])
  (:import jnr.ffi.LibraryLoader))

(definterface CruxRs
  (^jnr.ffi.Pointer c_version_string [])
  (^void c_string_free [^jnr.ffi.Pointer c_string]))

(defn -main [& args]
  (let [crux-rs ^CruxRs (.load (LibraryLoader/create CruxRs) "../../target/debug/libcrux.so")
        c-version-string (.c_version_string crux-rs)]
    (try
      (log/info (.getString c-version-string 0))
      (finally
        (.c_string_free crux-rs c-version-string)))))
