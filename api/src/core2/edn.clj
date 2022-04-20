(ns core2.edn
  (:require [time-literals.read-write :as time-literals.rw]
            [clojure.string :as str])
  (:import java.io.Writer
           [java.time Duration Instant ZonedDateTime Period]
           [org.apache.arrow.vector PeriodDuration]))

(defn duration-reader [s] (Duration/parse s))
(defn instant-reader [s] (Instant/parse s))
(defn zdt-reader [s] (ZonedDateTime/parse s))
(defn period-duration-reader [pd]
  (let [[p d] (str/split pd #" ")]
    (PeriodDuration. (Period/parse p) (Duration/parse d))))


(defn- print-time-to-string [t o]
  (str "#time/" t " \"" (str o) "\""))

(when-not (System/getenv "CORE2_DISABLE_EDN_PRINT_METHODS")

  (time-literals.rw/print-time-literals-clj!)

  (defmethod print-dup Duration [^Duration d, ^Writer w]
    (.write w (format "#c2/duration \"%s\"" d)))

  (defmethod print-method Duration [d w]
    (print-dup d w))

  (defmethod print-dup Instant [^Instant i, ^Writer w]
    (.write w (format "#c2/instant \"%s\"" i)))

  (defmethod print-method Instant [i w]
    (print-dup i w))

  (defmethod print-dup ZonedDateTime [^ZonedDateTime zdt, ^Writer w]
    (.write w (format "#c2/zdt \"%s\"" zdt)))

  (defmethod print-method ZonedDateTime [zdt w]
    (print-dup zdt w))

  (defmethod print-dup PeriodDuration [c ^Writer w]
    (.write w ^String (print-time-to-string "period-duration" c)))

  (defmethod print-method PeriodDuration [c ^Writer w]
    (print-dup c w)))
