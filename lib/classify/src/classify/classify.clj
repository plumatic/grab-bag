(ns classify.classify
  "Code for training (currently) binary classifiers.  Mid-refactor pulling out more generic
   versions in classify.learn."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.logging :as log]
   [plumbing.math :as math]
   [plumbing.parallel :as parallel]
   [flop.math :as flop-math]
   flop.weight-vector
   [classify.algorithms.max-ent :as max-ent]
   [classify.core :as classify]
   [classify.features :as features]
   [classify.learn :as learn])
  (:import
   [flop LongDoubleFeatureVector]
   [flop.weight_vector SparseWeightVector]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Converting raw data into training examples

;; Raw data is seq of [doc label] pairs.
;; Featurized data is seq of [ldfv label] pairs.

(def FeaturizedLabeledDatum
  [(s/one LongDoubleFeatureVector "feats")
   (s/one s/Any "label")
   (s/optional Double "weight")
   (s/optional s/Any "unique identifier for datum, reported in evaluator.")])

(s/defn featurize-data :- [FeaturizedLabeledDatum]
  [feature-set global-context raw-data :- [learn/LabeledDatum]]
  (learn/featurize-data #(features/feature-vector feature-set global-context %) raw-data))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Classification-specific stuff

(defprotocol PClassifier
  ;; extends learn/Model
  (posteriors [this datum]))

(defn keywise-merge-with
  "Same as merge-with, but f takes all the entries to merge at once rather than pairwise."
  [f & maps]
  (->> maps
       aconcat
       (group-by key)
       (map-vals #(apply f (map val %)))))

(s/defschema ClassificationEvaluation
  {(s/pair s/Any "gold label" s/Any "guess label")
   [(s/pair s/Any "datum ID" double "weight")]})

(defn confusion-weights [confusions]
  (map-vals #(sum second %) confusions))

(def classification-evaluator
  (reify learn/PEvaluator
    (evaluate [this model labeled-datum]
      (let [[d gold w id] labeled-datum]
        {[gold (learn/label model d)] [[id (or w 1.0)]]}))
    (combine [this evaluations]
      (apply keywise-merge-with concat evaluations))
    (display [this confusion]
      (if (every? #(contains? #{true false} %) (aconcat (keys confusion)))
        (let [confusion (confusion-weights confusion)
              tt (get confusion [true true] 0)
              tf (get confusion [true false] 0)
              ft (get confusion [false true] 0)
              ff (get confusion [false false] 0)
              p (double (math/safe-div tt (+ tt ft)))
              r (double (math/safe-div tt (+ tt tf)))]
          {:precision p
           :recall r
           :f1 (math/safe-div (* 2 p r) (+ p r))})
        (confusion-weights confusion) ;; TODO: make a nice table, other stats.
        ))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Maxent

(def +default-maxent-opts+
  {:normalize? false
   ;;:pred-prop 0.01
   :pred-thresh 3
   :unpack? true
   :sigma-sq 1.0
   :threads 32
   :print-progress true})

(defrecord MaxentModel [linear-classifier]
  learn/PModel
  (label [this datum] (classify/best-guess linear-classifier datum))

  PClassifier
  (posteriors [this datum] (classify/probs linear-classifier datum)))

(defn maxent-trainer [train-opts]
  (s/fn [train-data :- [FeaturizedLabeledDatum]]
    (when (:print-progress train-opts)
      (printf "training on %s data %s with opts %s\n"
              (count train-data) (frequencies (map second train-data)) train-opts))
    (MaxentModel.
     (->> (merge +default-maxent-opts+ train-opts)
          max-ent/trainer
          (#(% train-data))))))

(defn binary? [^MaxentModel m]
  (= #{true false}
     (set (keys (safe-get-in m [:linear-classifier :label->weights])))))

(defn model->nested-weight-maps [feature-set ^MaxentModel m]
  (map-vals
   #(features/fv->nested-features feature-set %)
   (safe-get-in m [:linear-classifier :label->weights])))

(defn model->true-weight-map [feature-set ^MaxentModel m]
  (let [maps (model->nested-weight-maps feature-set m)]
    (assert (binary? m))
    (safe-get maps true)))

(defn indexed-troves->model [indexed-troves]
  (->MaxentModel
   (classify/->LinearClassifier
    (map-vals #(SparseWeightVector. %) indexed-troves)
    false)))

(defn nested-weight-maps->model [feature-set nested-weight-maps]
  (->> nested-weight-maps
       (map-vals #(features/nested-features->trove feature-set %))
       indexed-troves->model))

(defn true-weight-map->model [feature-set nested-weight-map]
  (nested-weight-maps->model feature-set {true nested-weight-map false {}}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Maxent-EM

(s/defn perplexity [model data :- [LongDoubleFeatureVector]]
  (sum #(math/entropy (posteriors model %)) data))

(s/defn soft-label :- [FeaturizedLabeledDatum]
  [posteriors->weights data :- [LongDoubleFeatureVector] posteriors]
  (for [[datum posteriors] (map vector data posteriors)
        [label weight] (posteriors->weights posteriors)
        :when (> weight 0)]
    [datum label (double weight)]))

(defn hard-em-weighter [^double weight ^double cutoff]
  (fn [posteriors]
    (let [[best max-p] (apply max-key second posteriors)]
      (if (> max-p cutoff)
        {best weight}
        {}))))

(defn softish-em-weighter [^double weight ^double cutoff]
  (fn [posteriors]
    (let [[best max-p] (apply max-key second posteriors)]
      (if (> max-p cutoff)
        (map-vals #(* % weight) posteriors)
        {}))))

(s/defn em-trainer
  "Train a semi-supervised classifier using expectation maximization.
   Base-trainer is a trainer that creates a PClassifier.
   EM-opts control the EM procedure.

   TODO: opts to control # of unlabled examples, their weight,
   confidence bounding, etc."
  [base-trainer
   unlabeled-data :- [LongDoubleFeatureVector]
   & [em-opts]]
  (letk [[{num-iters 3}
          {print-progress true}
          {posteriors->weights identity}] em-opts]
    (s/fn [train-data :- [FeaturizedLabeledDatum]]
      (loop [model (base-trainer train-data)
             iter 1]
        (let [posts (pmap #(posteriors model %) unlabeled-data)]
          (when print-progress
            (log/infof "On EM iteration %s, unlabeled perplexity %s\nclass counts %s\n\n"
                       iter (sum #(math/entropy (vals %)) posts) (apply merge-with + posts)))
          (if (>= iter num-iters)
            model
            (recur
             (base-trainer (concat train-data (soft-label posteriors->weights unlabeled-data posts)))
             (inc iter))))))))

(set! *warn-on-reflection* false)
