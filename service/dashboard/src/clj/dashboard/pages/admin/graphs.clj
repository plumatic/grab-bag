(ns dashboard.pages.admin.graphs
  (:use plumbing.core)
  (:require
   [plumbing.new-time :as new-time]
   [store.bucket :as bucket]
   [store.mongo :as mongo]))


(defn combine [agg-fn datums]
  (apply map #(apply %1 %&) (cons max (repeat agg-fn)) datums))

;; TODO: should aggregate on time, not count -- this introduces weird distortions when there are gaps, etc.
(defn aggregate [agg-count agg-fn s]
  (when (seq s)
    (->> s #_(next s)
         (partition-all agg-count)
         (map (partial combine agg-fn))
         #_ (cons (first s)))))

(defn mean [& s] (/ (double (sum s)) (count s)))

(defnk generate-graph [log-data-store coll {limit (* 1440 7)} {des-pts 500} {agg-fn mean} & more]
  (let [data (mongo/query (mongo/bucket-collection log-data-store coll)
                          {:sort {"$natural" -1} :limit limit})
        date-cutoff (new-time/time-ago limit :mins)
        data (->> data
                  (remove (keyword ":date")) ; Correct for bad old data
                  (filter #(> (:date %) date-cutoff)))
        ks  (->> data
                 (map keys)
                 (apply concat)
                 set
                 (<- (disj :date :_id))
                 (sort-by (fn [k] (- (reduce #(max %1 (get %2 k 0)) 0 data))))
                 (cons :date))
        agg-count (or (:agg-count more) (max 1 (quot limit #_(count data) des-pts)))]
    (when (> (count ks) 1)
      (println "Generating graph with" coll limit (count data) des-pts agg-count)
      (->> data
           (map (fn [d] (map #(get d % 0) ks)))
           (?>> (> agg-count 1) (aggregate agg-count agg-fn))
           reverse
           (cons ks)))))

(defn preprocess-map [ops m]
  (reduce
   (fn [m [k op]]
     (if (contains? m k)
       (assoc m k (op (get m k)))
       m))
   (keywordize-map m)
   ops))

(defn list-metrics [log-data-store]
  (for-map [m (bucket/vals (mongo/bucket-collection log-data-store "meta"))
            :when (= (:type m) "metric")]
    (:full-name m)
    m))

(defn graph
  "Generate data for a metric graph from log-data-store"
  [log-data-store args]
  (generate-graph
   (assoc (preprocess-map
           {:limit #(Integer/parseInt %)
            :agg-count #(Integer/parseInt %)
            :des-pts #(Integer/parseInt %)
            :agg-fn {"+" + "first" first "max" max "min" min "mean" mean}}
           args)
     :log-data-store log-data-store)))

(defn graphs [log-data-store default-metrics args]
  (for-map [c (if-let [colls (get args :colls)] (.split ^String colls ",") default-metrics)
            :let [g (graph log-data-store (assoc args :coll c))]
            :when g]
    c g))
