(ns classify.index
  (:use plumbing.core)
  (:require
   [hiphip.double :as d]
   [plumbing.index :as index]
   [plumbing.io :as io]
   [plumbing.map :as map]
   [plumbing.math :as math]
   [plumbing.parallel :as parallel]
   [schema.core :as s]
   [flop.weight-vector :as weight-vector])
  (:import
   [plumbing.index Index]
   [gnu.trove TLongDoubleHashMap]
   [flop IObjectWeightVector IWeightVector LongDoubleFeatureVector]))

(set! *warn-on-reflection* true)

(defn convert-preds
  [preds ^Index p-index normalize?]
  (let [out (LongDoubleFeatureVector. (count preds))]
    (doseq [[p v] (seq preds)
            :let [ip (index/get-index p-index p)]
            :when (>= ip 0)]
      (assert (not (Double/isNaN (double v))))
      (.increment out ip v))
    (.compact out)
    (let [n (.norm out)]
      (when (> n 0.0)
        (if normalize?
          (.scaleInPlace out (/ 1.0 n))
          out)))))

(defn pair [x x-name y y-name]
  [(s/one x x-name) (s/one y y-name)])

(def PredValPairs (s/either flop.LongDoubleFeatureVector
                            java.util.Map
                            [(pair s/Any "pred" Number "val")]))
(def LabeledDatum (pair PredValPairs "datum" s/Any "label"))

(def WeightedDatum
  (conj LabeledDatum (s/one double "datum-weight")))

(defn distinct-keys [datum]
  (if (or (instance? flop.LongDoubleFeatureVector datum)
          (map? datum))
    (keys datum)
    (distinct-fast (map first datum))))

(let [+rand+ (java.util.Random. 0)]
  (defn build-pred-index
    "process-datum-fn: returns [pred-vals label] pair
   data: seq of datums"
    [data
     {:keys [pred-thresh, num-threads count-prob pred-prop]
      :or {pred-thresh 0 num-threads 1 count-prob 1.0 pred-prop 0.0}}]
    (let [pred-thresh (max pred-thresh (* pred-prop (count data)))
          worker (fn [data]
                   (let [m (java.util.HashMap.)]
                     (doseq [[datum] data
                             pred (->> datum
                                       distinct-keys
                                       (?>> (< count-prob 1.0)
                                            (filter (fn [_] (< (.nextDouble +rand+) count-prob)))))]
                       (map/inc-key! m pred 1.0))
                     m))
          blocks (if (= num-threads 1)
                   [(worker data)]
                   (parallel/map-work
                    num-threads worker
                    (partition-all (math/ceil (/ (count data) num-threads)) data)))]
      (let [[^java.util.Map f & m] blocks]
        (doseq [m2 m
                [k v] (seq m2)]
          (map/inc-key! f k v))
        (index/static
         (for [[pred count] f
               :when (> count pred-thresh)]
           pred))))))

(defn convert-and-index-data
  [data opts]
  (let [p-index (build-pred-index data opts)
        i-data (pmap (fn [[preds label & [weight]]]
                       (let [cp (convert-preds preds p-index (:normalize? opts))]
                         [cp label (or weight 1.0)]))
                     data)]
    [(if (:allow-empty? opts)
       (map (fn-> (update-in [0] #(or % (LongDoubleFeatureVector.)))) i-data)
       (filter (fn [[p]] (if p
                           true
                           (println "WARNING: dropping empty datum")))
               i-data))
     p-index]))



(deftype IndexedWeightVector [^IWeightVector wv p-index]
  IWeightVector
  (dimension [this] (.dimension wv))
  (active-dimension [this] (.active-dimension wv))
  (^double val-at [this ^long item] (.val-at wv (long item)))
  (inc! [this item val] (throw (UnsupportedOperationException. "Don't be a lazy fuck.  Train on indexed data.")))
  (^double dot-product [this ^doubles other]
    (.dot-product wv other))
  (^double dot-product [this ^LongDoubleFeatureVector other]
    (.dot-product wv other))
  (reduce [this ^clojure.lang.IFn$OLDO f init] (.reduce wv f init))

  IObjectWeightVector
  (^double val-at [this ^Object item]
    (let [i (index/get-index p-index item)]
      (if (== i -1)
        0.0
        (.val-at this (long i)))))
  (^double dot-product [this ^java.util.Collection other]
    (if (= 0 (.active-dimension this))
      0.0
      (math/sum-od (fn ^double [[o v]] (* v (.val-at this o))) other)))
  (reduce [this ^clojure.lang.IFn$OODO f init]
    (.reduce wv (fn [^long l ^double d o] (f (index/item p-index l) d o)) init))
  (^LongDoubleFeatureVector index [this ^java.util.Collection other]
    (convert-preds other p-index false)) ;; does normalization matter?

  io/PDataLiteral
  (to-data [this] [::indexed-weight-vector (io/to-data wv) (io/to-data p-index)]))


(defmethod io/from-data ::indexed-weight-vector [[_ wv-data p-index-data]]
  (IndexedWeightVector. (io/from-data wv-data) (io/from-data p-index-data)))

(s/defn unindex-dense :- TLongDoubleHashMap
  [weights :- doubles p-index]
  (let [ldhm (TLongDoubleHashMap. (alength weights))]
    (d/doarr [[i w] weights]
             (.put ldhm (index/item p-index i) w))
    ldhm))

;; internal weight vector MUST be dense
(defn ^flop.weight_vector.SparseWeightVector unpack-dense [^IndexedWeightVector iwv]
  (weight-vector/->SparseWeightVector
   (unindex-dense
    (.xs ^flop.weight_vector.DenseWeightVector (.wv iwv))
    (.p-index iwv))))

(set! *warn-on-reflection* false)
