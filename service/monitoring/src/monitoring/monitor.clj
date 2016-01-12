(ns monitoring.monitor
  (:use plumbing.core)
  (:require
   [clojure.set :as set]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.timing :as timing]
   [flop.stats :as stats]
   [aws.ec2 :as ec2]
   [aws.elb :as elb]))


(defn map-similarity [val-sim? m1 m2]
  (let [common (set/intersection (set (keys m1)) (set (keys m2)))]
    (if (empty? common) 0.0
        (/ (count-when (fn [k] (val-sim? (m1 k) (m2 k))) common)
           (Math/max (count m1) (count m2))))))

(defprotocol Monitor
  (execute      [this] "Execute this test, update internal stats, and return error report.")
  (daily-report [this] "Return a full report and clear stats."))

(defn abbreviate [report]
  (if (> (count report) 15)
    (concat (take 10 report)
            [(format "... (%s more)" (- (count report) 15))]
            (take-last 5 report))
    report))

(defrecord SimpleMonitor [test-fn alert? time-threshold
                          daily-stats-atom daily-counts-atom
                          fetch-times-atom fail-seq-atom]
  Monitor
  (execute [this]
    (try
      (let [[fail tm] (timing/get-time-pair (let [f (future (test-fn))
                                                  res (deref f (min 60000 (* 2 time-threshold)) nil)]
                                              (when-not (future-done? f)
                                                (future-cancel f))
                                              res))]
        (if fail
          (do (swap! fail-seq-atom conj fail)
              (swap! daily-counts-atom update-in [:bad-data] (fnil inc 0)))
          (do (reset! fail-seq-atom nil)
              (reset! fetch-times-atom (cons tm (take 2 @fetch-times-atom)))
              (swap! daily-stats-atom stats/add-obs tm)
              (swap! daily-counts-atom update-in [:green] (fnil inc 0)))))
      (catch Throwable e
        (log/infof e "Error thrown while monitoring")
        (swap! fail-seq-atom conj (str e))
        (swap! daily-counts-atom update-in [:fetch-fail] (fnil inc 0))))
    (cond (try (alert? @fail-seq-atom) (catch Throwable t true))
          {:bad-data-or-fetch (abbreviate @fail-seq-atom) }

          (> (apply min (cons 100000 @fetch-times-atom)) time-threshold)
          {:too-slow @fetch-times-atom}))

  (daily-report [this]
    (let [result {:fetch-time-stats (stats/uni-report @daily-stats-atom)
                  :fetch-count-stats @daily-counts-atom
                  :latest-fetch-times @fetch-times-atom}]
      (reset! daily-stats-atom stats/+empty-uni-stats+)
      (reset! daily-counts-atom {})
      result)))

(defn constant-max-fails [^long n]
  (fn [fails]
    (> (count fails) n)))

(defn simple-monitor
  "Executes the test-fn every minute.
   Allows for a latency of max-latency-ms, after which it counts as a
   fail.  The max-consecutive-fails is the maximum number of tries (aka
   minutes) in a row that the test-fn can fail before triggering an
   error."
  [test-fn & [max-latency-ms max-consecutive-fails]]
  (SimpleMonitor.
   test-fn
   (if (fn? max-consecutive-fails) max-consecutive-fails
       (constant-max-fails (or max-consecutive-fails 1)))
   (or max-latency-ms 1000)
   (atom stats/+empty-uni-stats+)
   (atom {})
   (atom [0 0 0 0 0])
   (atom nil)))


(defrecord CompositeMonitor [monitor-map]
  Monitor
  (execute [this]
    (->> monitor-map
         ;; run all the monitors with at most 3 at a time, keeping only the fails
         (parallel/map-work
          3
          (fn [[k m]]
            (when-let [fail (try (execute m)
                                 (catch Throwable e (str "err running test:" e)))]
              [k fail])))
         (into {})))
  (daily-report [this]
    (for-map [[k m] monitor-map]
      k (try (daily-report m) (catch Throwable e (str "err generating report" e))))))

(defn composite-monitor [monitor-map]
  (CompositeMonitor. monitor-map))

(defn ring-instances [ec2-keys ring-name]
  (let [inst-data (map-from-vals :instanceId (ec2/describe-instances (ec2/ec2 ec2-keys)))]
    (for [{:keys [instanceId] :as ring-inst} (elb/instances ec2-keys ring-name)
          :when (elb/healthy? ring-inst)]
      (inst-data instanceId))))

(defn elb-fuckoff? [fail]
  (and (string? fail)
       (let [fail ^String fail]
         (or (= fail "Status Code: 503, AWS Service: AmazonElasticLoadBalancing, AWS Request ID: null, AWS Error Code: null, AWS Error Message: null")
             (.startsWith fail "java.lang.RuntimeException: org.apache.http.conn.ConnectTimeoutException: Connect to elasticloadbalancing.amazonaws.com/elasticloadbalancing.amazonaws.com")))))

(defn phony-elb-unhealthy? [fail]
  (and (sequential? fail)
       (every?
        (fn [f]
          (when-let [data (:unhealthy-ring-member f)]
            (and (= (:state data) "Unknown")
                 (= (:reasonCode data) "ELB"))))
        fail)))

(defn ring-alert? [simple-health-check fails]
  (or (>= (count fails) 10)
      (>= (count (remove #(or (elb-fuckoff? %) (phony-elb-unhealthy? %)) fails)) 5)
      (and (>= (count fails) 5)
           (not (simple-health-check)))))

(defn ring-monitor [ec2-keys ring-name min-instances simple-health-check ms-per-instance healthy-monitor-fn]
  (simple-monitor
   #(not-empty
     (let [inst-data (map-from-vals :instanceId (ec2/describe-instances (ec2/ec2 ec2-keys)))
           ring (elb/instances ec2-keys ring-name)]
       (merge
        (when (< (count ring) min-instances)
          {:too-few-instances (count ring)})
        (for-map [ring-member ring
                  :let [instance-id (:instanceId ring-member)
                        fail (if (not (elb/healthy? ring-member))
                               {:unhealthy-ring-member ring-member}
                               (let [[res ms] (timing/get-time-pair
                                               (healthy-monitor-fn (inst-data instance-id)))]
                                 (cond
                                  (seq res) res
                                  (> ms ms-per-instance) {:too-slow ms :limit ms-per-instance})))]
                  :when (seq fail)]
          instance-id fail))))
   150000
   #(ring-alert? simple-health-check %)))
