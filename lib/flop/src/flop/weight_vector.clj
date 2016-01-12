(ns flop.weight-vector
  (:use plumbing.core)
  (:require
   [hiphip.double :as d]
   [plumbing.io :as io]
   [flop.array :as fa]
   [flop.map :as fm])
  (:import
   [flop IWeightVector LongDoubleFeatureVector]
   [gnu.trove TLongDoubleHashMap]))

(set! *warn-on-reflection* true)

(deftype SparseWeightVector [^TLongDoubleHashMap ldhm]
  IWeightVector
  (dimension [this]
    (throw (RuntimeException. (str "Cant ask for dimension of sparse weight-vector"))))
  (active-dimension [this] (.size ldhm))
  (val-at [this idx] (.get ldhm idx))
  (inc! [this idx val] (.put ldhm idx (+ (.get ldhm idx) val)))
  (^double dot-product  [this ^LongDoubleFeatureVector other] (.dotProduct other ldhm))
  (^double dot-product [this ^doubles other]
    (let [it (.iterator ldhm)
          n (.size ldhm)]
      (loop [sum 0.0 idx 0]
        (if (>= idx n) sum
            (let [_ (.advance it)
                  k (.key it)
                  v (.value it)]
              (recur (+ sum (* v (d/aget other k))) (inc idx)))))))
  (reduce [this f init]
    (let [it (.iterator ldhm)
          n (.size ldhm)]
      (loop [ret init idx 0]
        (if (>= idx n) ret
            (let [_ (.advance it)
                  k (.key it)
                  v (.value it)]
              (recur (f ret k v) (inc idx)))))))

  io/PDataLiteral
  (to-data [this] [::sparse-weight-vector (fm/trove->map ldhm)]))

(defmethod io/from-data ::sparse-weight-vector [[_ data]]
  (SparseWeightVector. (fm/map->trove data)))

(deftype DenseWeightVector [^doubles xs]
  IWeightVector
  (dimension [this] (alength xs))
  (active-dimension [this] (alength xs))
  (val-at [this idx] (if (< idx 0) 0.0 (d/aget xs idx)))
  (inc! [this idx val] (d/ainc xs idx val))
  (^double dot-product [this ^doubles other]
    (d/dot-product xs other))
  (^double dot-product [this ^LongDoubleFeatureVector other]
    (.dotProduct other xs))
  (reduce [this f init] ;; TODO: this should be areduce now.
    (let [n (alength xs)]
      (loop [idx 0 ret init]
        (if (>= idx n)
          ret
          (recur (inc idx) (f ret idx (d/aget xs idx)))))))

  io/PDataLiteral
  (to-data [this] [::dense-weight-vector (seq xs)]))

(defmethod io/from-data ::dense-weight-vector [[_ data]]
  (DenseWeightVector. (double-array data)))


(defn map->sparse
  [m] (SparseWeightVector. (fm/map->trove (map-vals double m))))

(defn new-dense
  ([init] (DenseWeightVector. (double-array init))))

(defn ->map [^IWeightVector wv]
  (.reduce wv assoc {}))

(defn explain [^IWeightVector weights ^clojure.lang.IFn$LO get-pred ^LongDoubleFeatureVector pred-fv]
  (->> pred-fv
       .asMap
       (map (fn [[p v]] [(get-pred p) (* v (.val-at weights p))]))
       (into {})))

(defn inc-all! [^IWeightVector weights ^LongDoubleFeatureVector fv ^double scale]
  (.forEachEntry fv
                 (fn [^long idx ^double val]
                   (.inc! weights idx (* val scale)))))

(defn inc-all-wv! [^IWeightVector weights ^IWeightVector wv2 ^double scale]
  (.reduce wv2 (fn [_ ^long k ^double v] (.inc! weights k (* v scale))) nil))

(set! *warn-on-reflection* false)
