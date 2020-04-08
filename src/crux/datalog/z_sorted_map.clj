(ns crux.datalog.z-sorted-map
  (:require [crux.z-curve :as cz]
            [crux.byte-keys :as cbk]
            [crux.datalog :as d]
            [crux.datalog.parser :as dp]))

(def ^:private z-wildcard-min-bytes (byte-array Long/BYTES (byte 0)))
(def ^:private z-wildcard-max-bytes (byte-array Long/BYTES (byte -1)))
(def z-wildcard-range [z-wildcard-min-bytes z-wildcard-max-bytes])

(defn tuple->z-address ^bytes [value]
  (cz/bit-interleave (mapv cbk/->byte-key value)))

(defn z-sorted-map-insert [this tuple]
  (assoc this (tuple->z-address tuple) tuple))

(defn z-sorted-map-delete [this tuple]
  (dissoc this (tuple->z-address tuple)))

(defn- non-z-range-var-bindings [var-bindings]
  (vec (for [var-binding var-bindings]
         (if (dp/logic-var? var-binding)
           (if (some (comp #{'!=} first) (d/var-constraints var-binding))
             var-binding
             (with-meta var-binding nil))
           var-binding))))

(defn var-bindings->z-range [var-bindings]
  (let [min+max (for [var-binding var-bindings]
                  (if (dp/logic-var? var-binding)
                    (if-let [constraints (d/var-constraints var-binding)]
                      (reduce
                       (fn [[min-z max-z] [op value]]
                         [(case op
                            (>= >) (let [value-bs (if (= '> op)
                                                    (if (int? value)
                                                      (cbk/->byte-key (inc ^long value))
                                                      (cbk/inc-unsigned-bytes (cbk/->byte-key value)))
                                                    (cbk/->byte-key value))
                                         diff (.compare cbk/unsigned-bytes-comparator value-bs min-z)]
                                     (if (pos? diff)
                                       value-bs
                                       min-z))
                            = (cbk/->byte-key value)
                            min-z)
                          (case op
                            (<= <) (let [value-bs (if (= '< op)
                                                    (if (int? value)
                                                      (cbk/->byte-key (dec ^long value))
                                                      (cbk/dec-unsigned-bytes (cbk/->byte-key value)))
                                                    (cbk/->byte-key value))
                                         diff (.compare cbk/unsigned-bytes-comparator value-bs max-z)]
                                     (if (pos? diff)
                                       max-z
                                       value-bs))
                            = (cbk/->byte-key value)
                            max-z)])
                       [z-wildcard-min-bytes
                        z-wildcard-max-bytes]
                       constraints)
                      [z-wildcard-min-bytes z-wildcard-max-bytes])
                    (let [value-bs (cbk/->byte-key var-binding)]
                      [value-bs value-bs])))]
    [(cz/bit-interleave (mapv first min+max))
     (cz/bit-interleave (mapv second min+max))]))

(defn z-sorted-map-table-filter [this db var-bindings]
  (let [[^bytes min-z ^bytes max-z :as z-range] (var-bindings->z-range var-bindings)
        non-z-var-bindings (non-z-range-var-bindings var-bindings)
        dims (count var-bindings)
        after-max-z (some-> (subseq this > max-z) (first) (key))
        s ((fn step [^bytes z]
             (reduce
              (fn [acc [^bytes k v]]
                (cond
                  (identical? k after-max-z)
                  (reduced acc)

                  (cz/in-z-range? min-z max-z k dims)
                  (conj acc v)

                  :else
                  (if-let [^bytes bigmin (second (cz/z-range-search min-z max-z k dims))]
                    (reduced (concat acc (step bigmin)))
                    acc)))
              []
              (subseq this >= z)))
           min-z)]
    (d/table-filter s db non-z-var-bindings)))

(def z-sorted-map-prototype
  {'crux.datalog/insert
   z-sorted-map-insert
   'crux.datalog/delete
   z-sorted-map-delete
   'crux.datalog/table-filter
   z-sorted-map-table-filter})

(defn z-sorted-map? [m]
  (and (map? m) (= z-sorted-map-table-filter ('crux.datalog/table-filter (meta m)))))

(defn new-z-sorted-map-relation [relation-name]
  (vary-meta (d/new-sorted-map-relation cbk/unsigned-bytes-comparator relation-name)
             merge
             z-sorted-map-prototype))
