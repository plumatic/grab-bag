(ns model-explorer.models
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.hash :as hash]
   [plumbing.index :as index]
   [plumbing.parallel :as parallel]
   flop.weight-vector
   [classify.classify :as classify]
   [classify.learn :as learn]
   [store.bucket :as bucket]
   [model-explorer.core :as model-explorer]
   [model-explorer.query :as query])
  (:import
   [flop LongDoubleFeatureVector]))

(defn labels
  "Given a model info, return the labels"
  [model-info]
  (keys (safe-get-in model-info [:training-data-params :training-labels])))

(defn featurize-nested-map [indexer nested-feature-map]
  (let [m (LongDoubleFeatureVector.)]
    (doseq [[coarse fine-map] nested-feature-map
            :let [coarse-hash (hash/murmur64 (str coarse))]
            [fine v] fine-map
            :when (not (zero? v))
            :let [k (bit-xor coarse-hash (hash/murmur64 fine))]]
      (bucket/put indexer k [coarse fine])
      (.put m k v))
    m))

(s/defn featurize-data :- [(s/pair s/Any "datum" LongDoubleFeatureVector "features")]
  [feature-index model data]
  (let [query-fn (-> model (safe-get :query) query/query-map->fn)]
    (parallel/map-work
     20
     nil
     (fn [d]
       [d
        (featurize-nested-map
         feature-index
         (assoc (query-fn d)
           [:bias] {"bias" 1.0}))])
     data)))

(defn train [model-info training-data all-datums]
  (letk [[[:training-data-params training-labels] trainer-params] model-info
         feature-index (bucket/bucket {})
         all-fvs (for-map [[d fv] (featurize-data feature-index model-info all-datums)]
                   (safe-get d :id) fv)
         labeled-featurized-data (for [[class-label data-labels] training-labels
                                       data-label data-labels
                                       datum (model-explorer/data training-data data-label)]
                                   [(safe-get all-fvs (:id datum)) class-label])
         train-result (learn/estimate-perf-and-train-model
                       {:trainer (classify/maxent-trainer (merge {:print-progress false} trainer-params))
                        :evaluator classify/classification-evaluator
                        :data labeled-featurized-data})]
    {:date (millis)
     :model-info model-info
     :train-info train-result
     :auto-tags (for-map [[id fv] all-fvs]
                  id (classify/posteriors (:model train-result) fv))
     :feature-index feature-index}))

(defnk datum->auto-labels-fn [model->train-info :as model-graph]
  (let [all-trained (bucket/vals model->train-info)]
    (fnk [type id :as datum]
      (apply merge
             (for [trained all-trained
                   :when (= type (safe-get-in trained [:model-info :data-type]))]
               (get-in trained [:auto-tags id]))))))

(def model-graph
  (graph/graph
   ;; :train-evaluation :evaluation :model :final-train-evaluation :auto-tags
   :model->train-info (graph/instance bucket/bucket-resource {:type :mem})))

(defn model-key [datatype model-id]
  [datatype model-id])

(defn get-model [model-graph datatype model-id]
  (bucket/get (safe-get model-graph :model->train-info) (model-key datatype model-id)))

(defn retrain!
  [model-graph training-data list-data model-info]
  (letk [[model->train-info] model-graph
         [data-type id] model-info
         training-data-store (safe-get training-data data-type)]
    (bucket/put
     model->train-info (model-key data-type id)
     (train model-info training-data-store (list-data data-type)))))
