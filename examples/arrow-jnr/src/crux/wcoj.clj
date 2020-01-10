(ns crux.wcoj
  (:require [clojure.string :as s]))

;; Simplistic spike using binary strings for z-order to
;; explore the algorithms described in the papers:

;; Optimal Joins using Compact Data Structures
;; https://arxiv.org/abs/1908.01812
;; Worst-Case Optimal Radix Triejoin
;; https://arxiv.org/abs/1912.12747
;; https://brodyf.github.io/thesis.pdf

(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic ^{:tag 'long} *binary-str-length* Long/SIZE)

(defn ->binary-str ^String [^long i]
  (s/replace
   (format (str "%" *binary-str-length* "s") (Long/toUnsignedString i 2))
   \space \0))

(defn parse-binary-str ^Number [^String b]
  (if (> (count b) Long/SIZE)
    (BigInteger. b 2)
    (Long/parseUnsignedLong b 2)))

(defn dimension ^long [^String b]
  (quot (count b) *binary-str-length*))

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

(defn component [^String b ^long idx]
  (s/join (take-nth (dimension b) (drop idx b))))

(defn components [^String b idxs]
  (for [i idxs]
    (component b i)))

;; An example database for this query on domain {0, 1, 2} is R(A, B) =
;; {(0, 1)}, S(B,C) = {(1, 2)}, T(A, C) = {(0, 2)} and a possible
;; Booleanisation of the relations could be R(2) (A0, A1, B0, B1) =
;; {(0, 0, 0, 1)}, S(2) (B0, B1 , C0, C1) = {(0, 1, 1, 0)}, T(2)(A0,
;; A1, C0, C1) = {(0, 0, 1, 0)}

(comment
  (let [q (interleave-bits [0 1 2])
        r (interleave-bits [0 1])
        s (interleave-bits [1 2])
        t (interleave-bits [0 2])]))
