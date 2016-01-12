(ns classify.algorithms.max-ent
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [hiphip.double :as dbl]
   [plumbing.index :as plumbing-index]
   [plumbing.logging :as log]
   [plumbing.math :as math]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [flop.array :as fa]
   [flop.map :as map]
   [flop.optimize :as optimize]
   [flop.weight-vector :as weight-vector]
   [classify.core :as classify]
   [classify.index :as index]
   [classify.utils :as utils])
  (:import
   [flop LongDoubleFeatureVector]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(definterface IFuckThisShit
  (updategrad [^doubles grad ^long pred ^double val ^doubles probs]))

(defprotocol ClassifierHelper
  (update-emp-counts! [this ^doubles emp-counts i-preds ^long i-label ^double weight])
  (^doubles -sums [this weight-arr i-preds])
  (^doubles -log-probs [this weight-arr i-preds])
  (^doubles -probs [this weight-arr i-preds])
  (dimension [this])
  (weight [this ^doubles weight-arr i-pred i-label])
  (-explain [this ^doubles weight-arr datum]))

(let [da (class (double-array []))]
  (defn  log-helper-score-preds
    ^double [weights ^LongDoubleFeatureVector pred-vals]
    (if (instance? da weights)
      (.dotProduct pred-vals ^doubles weights)
      (.dotProduct pred-vals ^floats weights))))

(defn double-array-pair [^double d1 ^double d2]
  (doto (double-array 2)
    (dbl/aset 0 d1)
    (dbl/aset 1 d2)))

(defrecord LogisticHelper
    [num-preds num-labels]

  IFuckThisShit
  (updategrad
    [this grad p v probs]
    (dbl/ainc grad p (* v (aget probs 0))))

  ClassifierHelper
  (update-emp-counts!
    [this emp-counts i-preds i-label weight]
    (when (= 0 i-label)
      (map/do-fv [[p v] i-preds] (dbl/ainc emp-counts p (* weight v)))))

  (-sums
    [this weight-arr i-preds]
    (let [sum (log-helper-score-preds weight-arr i-preds)]
      (double-array-pair sum 0.0)))

  (-log-probs
    [this weight-arr i-preds]
    (let [sum (log-helper-score-preds weight-arr i-preds)
          expSum (Math/exp (- sum))
          lp-true (- (Math/log (+ 1.0 expSum)))
          lp-false (- lp-true sum)]
      (double-array-pair lp-true lp-false)))

  (-probs
    [this weight-arr i-preds]
    (let [sum (log-helper-score-preds weight-arr i-preds)
          expSum (Math/exp (- sum))
          p-true (/ 1.0 (+ 1.0 expSum))
          p-false (- 1.0 p-true)]
      (double-array-pair p-true p-false)))

  (dimension [this] num-preds)

  (weight [this weight-arr ipred ilabel]
    (if (= ilabel 0) (dbl/aget weight-arr ipred) 0.0))

  (-explain [this weight-arr datum]
    (for-map [l (range num-labels)]
      l (for-map [[p v] datum]
          p (* v (dbl/aget weight-arr p) (if (= l 0) 1 -1))))))



(defn pred-label-to-index
  ^long [^long num-labels ^long pred ^long label]
  (+ (* pred num-labels) label))

(defrecord MaxEntHelper
    [^long num-preds ^long num-labels]

  IFuckThisShit
  (updategrad
    [this grad p v probs]
    (dbl/doarr [[label prob] probs]
               (dbl/ainc grad
                         (pred-label-to-index num-labels p label)
                         (* v prob))))

  ClassifierHelper
  (update-emp-counts!
    [this emp-counts i-preds i-label weight]
    (map/do-fv [[p v] i-preds]
               (let [idx (pred-label-to-index num-labels p i-label)]
                 (dbl/ainc emp-counts idx (* weight v)))))

  (-sums
    [this weight-arr i-preds]
    (let [sums (double-array num-labels 0.0)]
      (map/do-fv [[p v] i-preds]
                 (assert (< p num-preds))
                 (dotimes [l num-labels]
                   (let [idx (pred-label-to-index num-labels p l)
                         w (dbl/aget weight-arr idx)]
                     (dbl/ainc sums l (* v w)))))
      sums))

  (-log-probs [this weight-arr i-preds]
    (let [sums (-sums this weight-arr i-preds)]
      (fa/log-normalize-in-place! sums)))

  (-probs [this weight-arr i-preds]
    (let [log-probs (-log-probs this weight-arr i-preds)]
      (dbl/afill! [[i v] log-probs] (Math/exp v))))

  (dimension [this] (* num-preds num-labels))

  (weight [this weight-arr ipred ilabel]
    (dbl/aget weight-arr (pred-label-to-index num-labels ipred ilabel)))


  (-explain [this weight-arr datum]
    (for-map [l (range num-labels)]
      l (for-map [[p v] datum]
          p (* v (weight this weight-arr p l))))))

(defn new-helper
  [num-preds num-labels]
  (if (= 2 num-labels)
    (LogisticHelper. num-preds num-labels)
    (MaxEntHelper. num-preds num-labels)))

(defn emp-counts
  [i-data ^classify.algorithms.max_ent.ClassifierHelper helper]
  (let [counts-arr (double-array (dimension helper) 0.0)]
    (doseq [[i-preds i-label weight] i-data
            :let [i-label (long i-label)]]
      (.update-emp-counts! helper ^doubles counts-arr i-preds i-label weight))
    counts-arr))

(defn obj-fn-worker [^classify.algorithms.max_ent.ClassifierHelper helper weight-arr i-data]
  (let [grad (double-array (dimension helper))
        neg-log-prob (math/sum-od
                      (fn ^double [o]
                        (let [i-preds (first o)
                              i-label (long (second o))
                              weight (double (nth o 2))
                              ;; To avoid creating a new array
                              ;; to pass to .updategrad, scale-in-place! here
                              ;; and subtract log(weight) from log-likelihood
                              probs ^doubles (-probs helper weight-arr i-preds)
                              obj-val (- (* weight (Math/log (dbl/aget probs i-label))))]
                          (fa/scale-in-place! probs weight)
                          (map/do-fv [[p v] i-preds] (.updategrad ^IFuckThisShit helper grad p v probs))
                          obj-val))
                      i-data)]
    (list neg-log-prob grad)))

(defn obj-fn
  [i-data helper pool threads]
  (let [emp-counts (emp-counts i-data helper)
        dim (dimension helper)
        iter (atom 0)
        i-data-blocks (mapv vec (partition-all (math/ceil (/ (count i-data) threads)) i-data))]
    (log/debugf "%s Training Data [%s threads]" (count i-data) threads)
    (fn [weight-arr]
      (swap! iter inc)
      (let [grad-arr (fa/scale emp-counts -1)
            results (parallel/map-work
                     (or pool threads)
                     (fn [i-data-block]
                       (obj-fn-worker helper weight-arr i-data-block))
                     i-data-blocks)
            neg-log-prob (sum first results)]
        (doseq [[_ block-grad] results]
          (fa/add-in-place! grad-arr block-grad 1.0 0.0))
        [neg-log-prob grad-arr]))))

(defn ->weights [^doubles dense-weights p-index unpack?]
  (let [wv (index/->IndexedWeightVector (weight-vector/new-dense dense-weights) p-index)]
    (if unpack? (index/unpack-dense wv) wv)))

(defn unsplat [^doubles arr l-index p-index unpack?]
  (if (= 2 (count l-index))
    {(first l-index) (->weights arr p-index unpack?)
     (second l-index) (->weights (double-array 0) p-index unpack?)}
    (for-map [[li l] (indexed l-index)]
      l (let [ds (double-array (/ (alength arr) (count l-index)))]
          (dotimes [pi (alength ds)]
            (aset ds pi (aget arr (pred-label-to-index (count l-index) pi li))))
          (->weights ds p-index unpack?)))))

(defn fn-val-arr [p-index ^clojure.lang.IFn$OD f]
  (dbl/amake [i (count p-index)] (.invokePrim f (plumbing-index/item p-index i))))

(defn inv-sigma-sq-arr [p-index sigma-sq-fn]
  (fn-val-arr p-index (fn ^double [k] (/ 1.0 (sigma-sq-fn k)))))

(defn prior-weight-arr [p-index ^flop.IWeightVector prior]
  (fn-val-arr p-index (fn ^double [k] (.val-at prior k))))

(def IndexedDatum
  [(s/one LongDoubleFeatureVector "predicate-values")
   (s/one long "label-index")
   (s/one double "datum-weight")])

(s/defn compute-means-and-std-devs :- (s/pair doubles "pred-means" doubles "pred-std-devs")
  [num-preds :- long pred-val-vectors :- [LongDoubleFeatureVector]]
  (let [;; Initially sum x_i
        means (double-array num-preds)
        ;; Initially sum x_i^2
        sum-x-squared (double-array num-preds)
        N (count pred-val-vectors)]
    (doseq [[^LongDoubleFeatureVector pred-vals] pred-val-vectors]
      (map/do-fv [[p v] pred-vals]
                 (dbl/ainc means p v)
                 (dbl/ainc sum-x-squared p (* v v))))

    [(fa/scale-in-place! means (/ 1.0 N))
     (dbl/amap [[p x-sq] sum-x-squared x-bar means]
               (Math/sqrt (+ 0.0001 (- (/ x-sq N) (* x-bar x-bar)))))]))

(defn norm-pred-vals [^LongDoubleFeatureVector pred-vals
                      ^doubles means
                      ^doubles std-devs]
  (let [out (LongDoubleFeatureVector. (.size pred-vals))]
    (map/do-fv [[p v] pred-vals]
               (.put out p (/ (- v (dbl/aget means p))
                              (dbl/aget std-devs p))))
    out))

(s/defn column-normalize
  [i-data :- [IndexedDatum] means :- doubles std-devs :- doubles]
  (map (fn [[pred-vals label-idx weight]]
         [(norm-pred-vals pred-vals means std-devs)
          label-idx
          weight])
       i-data))

(defnk index-data [data :- [(s/either index/LabeledDatum index/WeightedDatum)]
                   {labels nil}
                   {threads 1}
                   {normalize? false}
                   {average? false}
                   {column-normalize? false}
                   :as opts]
  (let [labels (let [l (or labels (distinct (map second data)))] ;; if labels are true/false make true get the weights
                 (if (= #{true false} (set l)) [true false] l))
        l-index (plumbing-index/static labels)
        [pred-indexed-data p-index] (index/convert-and-index-data
                                     data (merge opts {:num-threads threads :normalize? normalize?}))
        sum-weights (if average? (sum last pred-indexed-data) 1.0)
        i-data (map (fn [[p l w]]
                      (let [new-weight (/ w sum-weights)]
                        (assert (pos? new-weight) (str "Data weights need to be positive: " new-weight))
                        [p (plumbing-index/index! l-index l) new-weight]))
                    pred-indexed-data)
        ;; Compute Column mean/std-devs
        [means std-devs] (when column-normalize?
                           (compute-means-and-std-devs (count p-index) i-data))]
    {:p-index p-index
     :std-devs std-devs
     :l-index l-index
     :i-data (if column-normalize?
               (column-normalize i-data means std-devs)
               i-data)}))

(defn trainer [{:as opts
                :keys [reg-fn optimizer threads print-progress unpack? normalize? labels
                       prior-weights sigma-sq sigma-sq-fn average? column-normalize?]
                :or {print-progress false
                     threads 1
                     optimizer optimize/lbfgs-optimize
                     unpack? false}}]
  (s/fn [data :- [(s/either index/LabeledDatum index/WeightedDatum)]]
    (letk [[p-index l-index i-data std-devs] (index-data (assoc opts :data data))]
      (let [helper (new-helper (count p-index) (count l-index))
            init-weight-arr (if prior-weights
                              (prior-weight-arr p-index prior-weights)
                              (double-array (dimension helper) 0.0))]
        (resource/with-open [obj-fn-pool (when (> threads 1)
                                           (parallel/fixed-thread-pool threads))]
          (let [obj-fn (obj-fn i-data helper obj-fn-pool threads)
                reg-obj-fn (cond
                            reg-fn
                            (reg-fn obj-fn opts)

                            (or prior-weights sigma-sq-fn)
                            (utils/fancy-l2-reg-fn
                             obj-fn init-weight-arr
                             (inv-sigma-sq-arr p-index (or sigma-sq-fn (fn [_] (or sigma-sq 1.0)))))

                            :else
                            (utils/l2-reg-fn obj-fn (or sigma-sq 1.0)))
                weight-arr (optimizer reg-obj-fn init-weight-arr opts)]
            (when column-normalize?
              (fa/multiply-in-place!
               weight-arr
               (dbl/amap [sigma std-devs] (/ 1.0 sigma))))
            (classify/->LinearClassifier (unsplat weight-arr l-index p-index unpack?) (not unpack?))))))))

(defn pair->datum [[^LongDoubleFeatureVector fv1 ^LongDoubleFeatureVector fv2 & [weight]]]
  (concat
   [(doto (LongDoubleFeatureVector. (+ (count fv1) (count fv2))) (.incrementAll fv1 1.0) (.incrementAll fv2 -1.0))
    true]
   (when weight [weight])))

(defn rank-trainer [opts]
  (let [binary-trainer (trainer (assoc opts :labels [true false]))]
    (fn [data-pairs]
      (binary-trainer (map pair->datum data-pairs)))))


(set! *warn-on-reflection* false)
