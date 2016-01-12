(ns classify.linear
  "Training and evaluation for linear models."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.index :as index]
   [flop.map :as map]
   [flop.stats :as stats]
   flop.weight-vector
   [classify.algorithms.regression :as regression]
   [classify.learn :as learn])
  (:import
   [flop.weight_vector SparseWeightVector]
   [flop LongDoubleFeatureVector]))


(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Ordinary linear predictors with l2 norm

(def l1-evaluator
  (reify learn/PEvaluator
    (evaluate [this model labeled-datum]
      (let [[d gold w] labeled-datum]
        (* (or w 1.0)
           (Math/abs (double (- gold (learn/label model d)))))))
    (combine [this evaluations] (sum evaluations))
    (display [this evaluation] evaluation)))

(def l2-evaluator
  (reify learn/PEvaluator
    (evaluate [this model labeled-datum]
      (stats/weighted-stats
       (let [[d gold w] labeled-datum]
         [[(- gold (learn/label model d)) (or w 1)]])))
    (combine [this evaluations] (reduce stats/merge-stats evaluations))
    (display [this evaluation] (stats/uni-report evaluation))))

(defnk rms-error [sum-sq-xs num-obs :as evaluation]
  (Math/sqrt (/ sum-sq-xs num-obs)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Simple averaging predictor

(defrecord ConstantModel [mean]
  learn/PModel
  (label [this datum] mean))

(defn weighted-mean [train-data]
  (safe-get
   (stats/uni-report
    (stats/weighted-stats (for [[_ label w] train-data] [label (or w 1)])))
   :mean))

(def +constant-trainer+
  (s/fn [train-data :- [learn/LabeledDatum]]
    (-> train-data
        weighted-mean
        (ConstantModel.))))

;; Simple model that learns means for each bucket, and falls back to overall mean if no data.
(defrecord BucketModel [bucket-fn mean-map overall-mean]
  learn/PModel
  (label [this datum]
    (get mean-map (bucket-fn datum) overall-mean)))

(defn bucket-trainer [bucket-fn]
  (s/fn [train-data :- [learn/LabeledDatum]]
    (BucketModel.
     bucket-fn
     (map-vals weighted-mean (group-by #(bucket-fn (first %)) train-data))
     (weighted-mean train-data))))

(defprotocol Explainable
  (explain [this]))

(defrecord LinearModel [indexer feature-fn weight-vector]
  learn/PModel
  (label [this datum]
    (->> (assoc (feature-fn datum) :bias 1.0)
         (map-keys #(index/get-index indexer %))
         (map-vals double)
         map/map->fv
         (.dot_product ^SparseWeightVector weight-vector)))
  Explainable
  (explain [this]
    (->> (.ldhm ^SparseWeightVector weight-vector)
         map/trove->map
         (map-keys #(index/item indexer %)))))

(defn linear-trainer [feature-fn opts]
  (s/fn [train-data :- [learn/LabeledDatum]]
    (let [indexer (index/dynamic)
          indexed-data (vec
                        (for [[datum label w] train-data]
                          [(map-keys #(index/index! indexer %) (assoc (feature-fn datum) :bias 1))
                           label w]))]
      (index/lock! indexer)
      (LinearModel. indexer feature-fn (regression/learn-linear indexed-data opts)))))

(defrecord FeaturizedLinearModel [weight-vector]
  learn/PModel
  (label [this datum]
    (.dot_product ^SparseWeightVector weight-vector ^LongDoubleFeatureVector datum)))

(defn featurized-linear-trainer [opts]
  (s/fn [train-data :- [learn/LabeledDatum]]
    (FeaturizedLinearModel. (regression/learn-linear train-data opts))))

(set! *warn-on-reflection* false)
