(ns classify.metrics
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [flop.stats :as stats]))

(defprotocol Metric
  (score-datum [this datum]
    "Take a datum, and return a flop.stats/UnivariateStats"))

(defn eval-datum [metrics datum]
  (map-vals #(score-datum % datum) metrics))

(defn eval-data
  ([metrics data] (eval-data metrics data eval-datum))
  ([metrics data eval-datum-fn]
     (let [scores (pmap #(eval-datum metrics %) data)]
       (->> (distinct (mapcat keys scores))
            (pmap (fn [k] [k (let [metric-scores (map k scores)]
                               (->> scores
                                    (map k)
                                    (reduce stats/merge-stats)))]))
            (into {})))))
