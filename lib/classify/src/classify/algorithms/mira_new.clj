(ns classify.algorithms.mira-new
  (:use plumbing.core)
  (:require
   [plumbing.rank :as rank]
   [flop.weight-vector :as weight-vector]
   [classify.core :as classify]
   [classify.index :as index])
  (:import
   [flop IWeightVector LongDoubleFeatureVector]))

(set! *warn-on-reflection* true)

(def +empty-fv+ (LongDoubleFeatureVector. 0))

(defprotocol POnlineClassifierUpdater
  (update! [this ^PClassifier c ^LongDoubleFeatureVector fv label]))

(defrecord MiraUpdater [max-alpha lambda loss-fn]
  POnlineClassifierUpdater
  (update! [_ c fv gold-y]
    ;; classifier must be a linear classifier
    (let [^LinearClassifier c c
          label-scores (classify/scores c fv)
          [guess-y guess-score]
          (apply max-key
                 (fn [[y score]] (+ score (* lambda (loss-fn gold-y y))))
                 label-scores)]
      (if (= guess-y gold-y)
        [0 0.0]
        (let [loss (loss-fn gold-y guess-y)
              alpha (min max-alpha
                         (/ (- loss (- (label-scores gold-y) guess-score))
                            (* 2.0 (.norm ^LongDoubleFeatureVector fv))))]
          #_(println "multiclass" gold-y guess-y label-scores loss alpha )
          (assert (>= alpha 0.0))
          (-> c :label->weights (get gold-y)  (weight-vector/inc-all! fv alpha))
          (-> c :label->weights (get guess-y)  (weight-vector/inc-all! fv (- alpha)))
          #_(->> c :label->weights (map-vals plumbing.io/to-data) pprint)
          [1 loss])))))

(declare ->PairwiseMira pairwise-mira-update!)

(defrecord BinaryMiraUpdater [max-alpha lambda loss-fn use-pairwise?]
  POnlineClassifierUpdater
  (update! [_ c fv gold-y]
    ;; classifier must be a linear classifier
    (let [^LinearClassifier c c
          lw (seq (:label->weights c))
          _ (assert (= (count lw) 2))
          [[first-label first-wv] [second-label _]] lw
          dp (.dot-product ^IWeightVector first-wv ^LongDoubleFeatureVector fv)
          [guess-y guess-score] (if (> (+ (* lambda (loss-fn gold-y first-label)) (/ dp 2.0))
                                       (- (* lambda (loss-fn gold-y second-label)) (/ dp 2.0)))
                                  [first-label (/ dp 2.0)]
                                  [second-label (/ dp -2.0)])
          sign (double (if (= first-label gold-y) 1 -1))]
      (if use-pairwise?
        (pairwise-mira-update!
         (->PairwiseMira first-wv lambda max-alpha)
         fv +empty-fv+ (* sign (loss-fn gold-y guess-y))) ;; could be this
        (if (= guess-y gold-y) [0 0.0]
            (let [loss (loss-fn gold-y guess-y)
                  alpha (min max-alpha
                             (/ (- loss (- (* sign (/ dp 2.0)) guess-score))
                                (* 2.0 (.norm ^LongDoubleFeatureVector fv))))]
              #_(println "binary" gold-y guess-y first-label second-label loss alpha)
              #_(println "BIN" loss alpha sign (- (* sign dp) guess-score) (.norm ^LongDoubleFeatureVector fv))
              (-> first-wv (weight-vector/inc-all! fv (* alpha sign 2.0)))
              #_(->> lw (map-vals plumbing.io/to-data) pprint)
              [1 loss]))))))

(defnk trainer [{max-alpha 1} {lambda 0.0}
                {loss-fn (fn [x y] (if (= x y) 0.0 1.0))}
                {max-iters 150}
                {type :default}]
  (fn [i-data]
    (let [labels (distinct (map second i-data))
          mira (if (or (= type :multiclass) (and (= type :default) (> (count labels) 2)))
                 (MiraUpdater. max-alpha lambda loss-fn)
                 (BinaryMiraUpdater. max-alpha lambda loss-fn (= type :binary2)))
          c (classify/->LinearClassifier
             (for-map [l labels] l (weight-vector/map->sparse {})) false)]

      (loop [iter 0]
        (let [num-errs (sum (fn [[fv label]] (first (update! mira c fv label))) i-data)]
          (printf "mira at iter %s with %s errors\n" iter num-errs)
          (when (and (> num-errs 0) (< (inc iter) max-iters))
            (recur (inc iter)))))
      c)))

(defnk unindexed-trainer [:as args]
  (fn [data]
    (let [[i-data p-index] (index/convert-and-index-data data {})
          unindexed-trainer ((trainer args) i-data)]
      (classify/->LinearClassifier
       (map-vals (fn [ws] (index/->IndexedWeightVector ws p-index))
                 (:label->weights unindexed-trainer))
       true))))


(defrecord PairwiseMira [^IWeightVector wv ^double lambda ^double max-alpha])

(defn pairwise-mira-update! [^PairwiseMira mira ^LongDoubleFeatureVector good ^LongDoubleFeatureVector bad ^double loss]
  (if (< loss 0.0)
    (recur mira bad good (- loss))
    (or (letk [[^IWeightVector wv lambda max-alpha] mira]
          (when-not (zero? loss)
            (let [goods (.dot-product wv good)
                  bads (.dot-product wv bad)]
              (when (>= (+ bads (* (double lambda) loss)) goods)
                (let [alpha (min (double max-alpha)
                                 (/ (- loss (* 1 (- goods bads)))
                                    (+ 1.0e-100 (.diffNorm ^LongDoubleFeatureVector good ^LongDoubleFeatureVector bad))))]
                  #_(println "PW" loss alpha (if (zero? (count bad)) 1 -1) (- goods bads) (.diffNorm ^LongDoubleFeatureVector good ^LongDoubleFeatureVector bad))
                  (weight-vector/inc-all! wv good alpha)
                  (weight-vector/inc-all! wv bad (- alpha))
                  [1 loss])))))
        [0 0.0])))

(set! *warn-on-reflection* false)
