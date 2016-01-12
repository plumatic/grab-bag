(ns classify.probabilistic
  "Models that try to predict based on soft data (e.g., each datum has an expected
   output probability), via conversion to ordinary hard maxent."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.logging :as log]
   [flop.math :as math]
   [classify.algorithms.max-ent :as max-ent]
   [classify.core :as classify]
   [classify.features :as features]
   [classify.learn :as learn]
   [classify.classify :as classify-classify])
  (:import
   [flop LongDoubleFeatureVector]))


(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Data schema and evaluator for soft training

(def FeaturizedSoftLabeledDatum
  "Data labeled with probabilities to match, rather than binary labels"
  [(s/one LongDoubleFeatureVector "feats") (s/one double "probability") (s/optional Double "weight")])

(def conditional-entropy-evaluator
  "Evaluate total conditional entropy of data given model.
     TODO: propagate total weight as well,
           so we can report conditional entropy per datum."
  (reify learn/PEvaluator
    (evaluate [this model labeled-datum]
      (let [[feats prob weight] labeled-datum
            post (double (learn/label model feats))]
        (* weight
           (+ (if (= prob 0.0) 0.0 (* prob (- (math/log2 post))))
              (if (= prob 1.0) 0.0 (* (- 1 prob) (- (math/log2 (- 1 post)))))))))
    (combine [this evaluations] (reduce + evaluations))
    (display [this evaluation] evaluation)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constant model for testing

(defrecord ConstantModel [mean]
  learn/PModel
  (label [this datum] mean))

(defn avg-posterior [train-data]
  (/ (sum #(* (nth % 1) (nth % 2)) train-data) (sum #(nth % 2) train-data)))

(def +constant-trainer+
  (s/fn [train-data :- [learn/LabeledDatum]]
    (ConstantModel. (avg-posterior train-data))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Binary maxent model

(s/defn flatten-data :- [classify-classify/FeaturizedLabeledDatum]
  "Transform soft labeled data into ordinary hard labeled data for maxent."
  [data :- [FeaturizedSoftLabeledDatum]]
  (for [[feats prob weight] data
        :when (pos? weight)
        [label p] {true prob false (- 1 prob)}
        :when (pos? p)]
    [feats label (* p weight)]))

(defprotocol Explainable
  (explain [this feature-set]))

(defrecord SoftMaxentModel [linear-classifier]
  learn/PModel
  (label [this datum]
    (safe-get (classify/probs linear-classifier datum) true))
  Explainable
  (explain [this fs]
    (features/fv->nested-features fs (classify/good-weight-vector linear-classifier))))

(defnk soft-maxent-trainer
  "Takes feature set to avoid regularizing bias, treating feature with name :bias specially."
  [feature-set {sigma-sq 1.0} & train-opts]
  (s/fn [train-data :- [FeaturizedSoftLabeledDatum]]
    (log/infof "training on %s data with opts %s, avg weight %s and wtd posterior %s\n"
               (count train-data)
               (assoc train-opts :sigma-sq sigma-sq)
               (/ (sum #(nth % 2) train-data) (count train-data))
               (avg-posterior train-data))
    (->SoftMaxentModel
     (->> (merge classify-classify/+default-maxent-opts+
                 {:sigma-sq-fn (fn [k] (if (= (features/type-of feature-set k) :bias)
                                         1000000000.0
                                         sigma-sq))}
                 train-opts)
          max-ent/trainer
          (#(% (flatten-data train-data)))))))


(set! *warn-on-reflection* false)
