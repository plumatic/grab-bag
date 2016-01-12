(ns monitoring.monitors.machines
  "Monitoring machine stats"
  (:use plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [store.bucket :as bucket]
   [store.mongo :as mongo]
   [monitoring.monitor :as monitor]
   [monitoring.monitors.core :as monitors]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private helpers

;; alarms: jvm_heap above 0.9
;; cpu above 0.9
;; disk* above 0.8
;; file-descriptors above 0.5

(defn list-machines [lds]
  (for [m (bucket/vals (mongo/bucket-collection lds "meta"))
        :when (and (= (:type m) "metric")
                   (.endsWith ^String (:full-name m) "machine"))]
    (:full-name m)))

(def +default-alarms+
  {"jvm_heap" 0.95
   "cpu" 0.9
   "disk" 0.9
   "ram-used" 0.98
   "file-descriptors" 0.5})

(defn machine-alarms [alarms spec]
  (for-map [[k v] spec
            :let [[_ thresh] (->> alarms
                                  (filter #(.contains (name k) ^String (first %)))
                                  (sort-by (comp - count first))
                                  first)]
            :when (and thresh (> v thresh))]
    k v))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: monitor resources

(def monitor-resources
  (graph/graph
   :monitors
   (fnk [env log-data-store]
     (let [ms (list-machines log-data-store)]
       (log/infof "Monitoring machine stats for %s" (pr-str ms))
       (for-map [^String m ms
                 :when (= (.contains m "stage") (= :stage env))]
         (keyword m)
         (monitors/lowpri
          (monitor/simple-monitor
           (fn []
             (let [metric (monitors/latest-metric log-data-store m)
                   fired  (machine-alarms
                           (if (.contains m "api")
                             +default-alarms+
                             (dissoc +default-alarms+ "cpu"))
                           metric)]
               (when (seq fired) fired)))
           60000
           (if (.contains m "stage") 10 5))))))))
