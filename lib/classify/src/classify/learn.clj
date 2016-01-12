(ns classify.learn
  "Generic code for learning infrastructure, model and task-independent"
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.math :as math]
   [plumbing.parallel :as parallel]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public Schemas

(def Label
  "A label, typically true/false for classification, real-valued for regression, etc."
  s/Any)

(def Datum
  "A datum, could be anything."
  s/Any)

(s/defschema LabeledDatum
  [(s/one Datum "datum") (s/one Label "label") (s/optional Double "weight")])

(defprotocol PModel
  (label [this datum] "Predict a label from a datum"))

(s/defschema Model (s/protocol PModel))

(s/defschema Trainer
  (s/=> Model [LabeledDatum]))

(s/defschema Evaluation
  "Some representation of performance"
  s/Any)

(defprotocol PEvaluator
  (evaluate [this model labeled-datum] "Returns an evaluation")
  (combine [this evaluations] "Returns a single evaluation")
  (display [this evaluation] "Pretty-print results of evaluations"))

(s/defschema Evaluator (s/protocol PEvaluator))

(s/defschema Partitioner
  "A function that takes k and a sequence of data, and returns
   k partitions of the data into train and test sequences."
  (s/=> [(s/pair [s/Any] "train" [s/Any] "test")]
        (s/named long "k-folds")
        (s/named [s/Any] "data")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private Helpers

(defn split
  "split-type is a map with one of the following keys
   {:train-frac 0.5 :train-num 5 :test-num 5}"
  [split-type data]
  (split-at
   (cond (:train-frac split-type)
         (math/ceil (* (:train-frac split-type) (count data)))

         (:train-num split-type)
         (:train-num split-type)

         (:test-num split-type)
         (- (count data) (:test-num split-type))

         :else (assert false))
   data))

(defn cross-validation-partitions
  [k xs]
  (if (= k 1)
    [(split {:train-frac 0.5} xs)]
    (let [parts (pci/partition-evenly k xs)]
      (for [i (range k)]
        [(aconcat (concat (subvec parts 0 i) (subvec parts (inc i))))
         (nth parts i)]))))

(defn stratified-cross-validation-partitions
  "Partition the data into k partitions, each of which has have a roughly equal,
   pseudorandomly (but determinstically) chosen subset of examples with each label.
   Should reduce variance and provide a better estimate of performance,
   especially on multiclass problems where there are rare classes."
  [k xs]
  (->> xs
       (pci/shuffle-by (java.util.Random. 0xfeedface))
       (pci/distribute-by second)
       (cross-validation-partitions k)))

(defn grouped-cross-validation-partitions
  "In some cases, related datums should be kept together when partitioning (e.g.,
   when the labels are correlated and will be recieved together at evaluation time).

   This version is like cross-validation-partitions, but is guaranteed to keep
   labeled datums that map to the same value under k-fn together."
  [k-fn k xs]
  (->> xs
       (group-by k-fn)
       vals
       (cross-validation-partitions k)
       (map (fn [[train test]]
              [(aconcat train) (aconcat test)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: common models/adapters

(defrecord ConstantModel [label]
  PModel
  (label [this datum] label))

(defn constant-model [label]
  (ConstantModel. label))


(defn fn->model [f]
  (reify PModel (label [this datum] (f datum))))


(defrecord AdaptedModel [feat-fn model]
  PModel
  (label [this datum] (label model (feat-fn datum))))

(s/defn featurize-data
  "Featurize data with a generic feature-fn"
  [f raw-data :- [LabeledDatum]]
  (vec
   (parallel/map-work
    10
    nil
    (fn [datum] (update-in (vec datum) [0] f))
    raw-data)))

(defn adapted-trainer [feat-fn trainer]
  (fn->> (featurize-data feat-fn) trainer (->AdaptedModel feat-fn)))


(defrecord BucketModel [bucket-fn sub-models]
  PModel
  (label [this datum] (label (safe-get sub-models (bucket-fn datum)) datum)))

(defn bucketed-trainer [bucket-fn trainer]
  (fn->> (group-by #(bucket-fn (first %))) (map-vals trainer) (->BucketModel bucket-fn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: training/evaluation

(defn evaluate-all [evaluator model test-data]
  (combine evaluator (map #(evaluate evaluator model %) test-data)))

(defn p-evaluate-all
  "Parallel map-reduce-style evaluation."
  [evaluator model test-data & [num-threads]]
  (let [num-threads (or num-threads (parallel/available-processors))]
    (->> test-data
         (pci/partition-evenly num-threads)
         (parallel/map-work
          num-threads
          (fn->> (mapv #(evaluate evaluator model %)) (combine evaluator)))
         (combine evaluator))))

(s/defn train-and-eval :- {:model Model :evaluation Evaluation :train-evaluation Evaluation}
  [trainer :- Trainer
   evaluator :- Evaluator
   train-data :- [LabeledDatum]
   test-data :- [LabeledDatum]]
  (let [model (trainer train-data)]
    {:model model
     :train-evaluation (p-evaluate-all evaluator model train-data)
     :evaluation (p-evaluate-all evaluator model test-data)}))

(defn- transpose-list-of-maps
  "Transpose a list of maps with same keys into map of lists."
  [maps]
  (assert (apply = (map count maps)))
  (for-map [k (keys (first maps))]
    k
    (mapv #(safe-get % k) maps)))

(s/defn cross-validate
  [trainer :- Trainer
   evaluator :- Evaluator
   data-partitions :- [(s/pair [LabeledDatum] "train" [LabeledDatum] "test")]]
  (->> data-partitions
       (parallel/map-work 1 (fn [[train test]]
                              (pci/safe-select-keys (train-and-eval trainer evaluator train test)
                                                    [:train-evaluation :evaluation])))
       transpose-list-of-maps
       (map-vals #(combine evaluator %))))

(defnk estimate-perf-and-train-model
  [data :- [LabeledDatum]
   trainer :- Trainer
   evaluator :- Evaluator
   {k-fold 5}
   {partitioner :- Partitioner stratified-cross-validation-partitions}]
  (let [metrics (cross-validate trainer evaluator (partitioner k-fold data))
        model (trainer data)]
    (assoc metrics
      :model model
      :final-train-evaluation (p-evaluate-all evaluator model data))))


(set! *warn-on-reflection* false)
