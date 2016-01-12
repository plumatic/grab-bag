(ns classify.core
  (:use plumbing.core)
  (:require
   [plumbing.io :as io]
   [flop.array :as fa]
   [flop.weight-vector :as weight-vector])
  (:import
   [flop IObjectWeightVector IWeightVector
    LongDoubleFeatureVector]))

(set! *warn-on-reflection* true)

;; preds should be LongDoubleFeatureVectors, or if you use IndexedWeightVector then they
;; can be a seq of [pred val] pairs and the weight vector will d the indexing for you
;; WARNING: If you use LongDoubleFeatureVectors this expects taht they will already be normalized.
(defprotocol PClassifier
  (scores [this preds])
  (probs [this preds])
  (best-guess [this preds])
  (explain [this ^clojure.lang.IFn$LO get-pred preds] "{label {pred contribution-value}}")
  (weight [this ^long ipred label]))

(defprotocol OnlineClassifier
  (update! [this ^LongDoubleFeatureVector i-preds label]))

;; TODO : this could maybe be optimized by indexing once in scores for Object case
;; if we know taht all weight vectors share the same p-index.
(defrecord LinearClassifier
    [label->weights indexed?] ;; map {label IWeightVector}

  PClassifier
  (scores [this preds]
    (map-vals
     (fn [wv]
       (if-not indexed?
         (.dot-product ^IWeightVector wv ^LongDoubleFeatureVector preds)
         (.dot-product ^IObjectWeightVector wv ^java.util.Collection (seq preds))))
     label->weights))
  (probs [this i-preds]
    ;; Assumes log-linear
    (let [class-scores (scores this i-preds)
          log-z (fa/log-add (double-array (vals class-scores)))]
      (map-vals (fn [log-prob] (Math/exp (- (double log-prob) log-z))) class-scores)))
  (best-guess [this i-preds]
    (->> (scores this i-preds)
         (apply max-key second)
         first))
  (explain [this get-pred datum] "{label {pred contribution-value}}"
    (map-vals #(weight-vector/explain % get-pred datum) label->weights))
  (weight [this ipred label] (.val-at ^IWeightVector (label->weights label) ipred))

  io/PDataLiteral
  (to-data [this] [::linear-classifier (map-vals io/to-data label->weights) indexed?]))

(defmethod io/from-data ::linear-classifier [[_ data indexed?]]
  (->LinearClassifier (map-vals io/from-data data) indexed?))

(defn good-weight-vector ^IWeightVector [^LinearClassifier binary-linear-classifier]
  (let [l-w (.label->weights binary-linear-classifier)]
    (assert (= (count l-w) 2) (str "Binary classifiers should have 2 weight vectors, not " (count l-w)))
    (doto (weight-vector/map->sparse {})
      (weight-vector/inc-all-wv! (safe-get l-w true) 1.0)
      (weight-vector/inc-all-wv! (safe-get l-w false) -1.0))))


(set! *warn-on-reflection* false)
