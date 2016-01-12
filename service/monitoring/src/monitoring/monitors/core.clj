(ns monitoring.monitors.core
  "Helpers for writing monitors"
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.new-time :as new-time]
   [store.mongo :as mongo]
   [web.client :as client]
   [monitoring.monitor :as monitor]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(s/defschema MonitorSpec
  {:monitor (s/protocol monitor/Monitor)
   :tier s/Keyword
   :prod-only? Boolean})

(s/defschema MonitorGraph
  "A subgraph with monitors and required resources."
  {:monitors (s/=> {s/Keyword MonitorSpec}
                   {s/Keyword s/Any})
   s/Keyword s/Any})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Monitoring service tiers

(s/defn monitor-spec :- MonitorSpec
  [monitor tier prod-only?]
  {:monitor monitor :tier tier :prod-only? prod-only?})

(defn lowpri [m] (monitor-spec m :lowpri false))
(defn lowpri-prod-only [m] (monitor-spec m :lowpri true))

(defn highpri [m] (monitor-spec m :highpri false))
(defn highpri-prod-only [m] (monitor-spec m :highpri true))

(defn rad-lowpri [m] (monitor-spec m :rad-lowpri false))
(defn rad-highpri [m] (monitor-spec m :rad-highpri false))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helpers for common tasks

(defn check-range [[lb ub] value]
  (when-not (<= (or lb Double/NEGATIVE_INFINITY) value (or ub Double/POSITIVE_INFINITY))
    (format "%s not in range [%s %s]" value lb ub)))

(defn latest-metrics [lds coll k]
  (mongo/query (mongo/bucket-collection lds coll)
               {:sort {"$natural" -1} :limit k}))

(defn latest-metric [lds coll]
  (first (latest-metrics lds coll 1)))

(defn metric-threshold
  "Ensure the latest mongo metrics are within provided ranges."
  [lds metric max-age-mins allow-missing? ranges]
  (if-let [mongo-metric (latest-metric lds metric)]
    (let [age (new-time/time-since (:date mongo-metric) :mins)]
      (if (< age max-age-mins)
        (for-map [[k rnge] ranges
                  :let [v (get mongo-metric k)
                        err (if v (check-range rnge v) (when-not allow-missing? "missing data in mongo record"))]
                  :when err]
          k err)
        {:stale-metric (format "%s is %s mins old" metric age)}))
    {:missing-metric metric}))

(defn simple-health-check
  [url]
  (let [{:keys [status body]} (client/consume-response identity (client/fetch :get url))]
    (when-not (= status 200)
      {:non-200-resp status :body body})))
