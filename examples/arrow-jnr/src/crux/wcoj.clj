(ns crux.wcoj
  (:require [clojure.string :as s]))

;; Simplistic spike using binary strings for z-order to
;; explore the algorithms described in the papers:

;; Optimal Joins using Compact Data Structures
;; https://arxiv.org/abs/1908.01812
;; Worst-Case Optimal Radix Triejoin
;; https://arxiv.org/abs/1912.12747

(defn ->binary-str ^String [^long i]
  (s/replace
   (format "%64s" (Long/toUnsignedString i 2))
   \space \0))

(defn parse-binary-str ^Number [^String b]
  (if (> (count b) Long/SIZE)
    (BigInteger. b 2)
    (Long/parseUnsignedLong b 2)))

(defn dimension ^long [^String b]
  (quot (count b) Long/SIZE))

(defn interleave-bits ^String [bs]
  (->> (for [b bs]
         (cond-> b
           (int? b) (->binary-str)))
       (apply interleave)
       (s/join)))

(defn as-path
  ([^String b]
   (as-path b (dimension b)))
  ([^String b ^long d]
   (map s/join (partition d b))))

(defn select-in-path [p idxs]
  (for [p p]
    (s/join (map (vec p) idxs))))

(defn component
  ([^String b]
   (component b (dimension b)))
  ([^String b ^long d]
   (s/join (take-nth 2 (drop d b)))))

(defn components
  ([^String b idxs]
   (components b (dimension b) idxs))
  ([^String b ^long d idxs]
   (for [i idxs]
     (component b i))))
