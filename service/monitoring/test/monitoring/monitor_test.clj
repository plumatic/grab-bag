(ns monitoring.monitor-test
  (:use plumbing.core clojure.test plumbing.test monitoring.monitor)
  (:require
   [plumbing.new-time :as new-time]
   [monitoring.deploy :as deploy])
  )

(deftest test-monitoring
  (let [test-fn-atom (atom (constantly nil))
        m (simple-monitor #(@test-fn-atom) 1000 1)]

    (dotimes [_ 10]
      (is (nil? (execute m))))

    (let [{:keys [fetch-time-stats fetch-count-stats latest-fetch-times]} (daily-report m)]
      (is (= (count latest-fetch-times) 3))
      (is (= fetch-count-stats {:green 10}))
      (is (== 10 (:count fetch-time-stats)))
      (is (< (:min fetch-time-stats) 10)))

    (reset! test-fn-atom #(throw (RuntimeException. "fuck off")))
    (is (nil? (execute m)))
    (is (= [:bad-data-or-fetch] (keys (execute m))))
    (is (= [:bad-data-or-fetch] (keys (execute m))))
    (reset! test-fn-atom (constantly nil))
    (is (nil? (execute m)))
    (reset! test-fn-atom #(throw (RuntimeException. "fuck off")))
    (is (nil? (execute m)))
    (reset! test-fn-atom (constantly nil))
    (is (nil? (execute m)))
    (reset! test-fn-atom (constantly {:horse :cart}))
    (is (nil? (execute m)))

    (let [{:keys [fetch-time-stats fetch-count-stats latest-fetch-times]} (daily-report m)]
      (is (= fetch-count-stats {:green 2 :fetch-fail 4 :bad-data 1})))

    (reset! test-fn-atom (constantly nil))
    (is (nil? (execute m))))
  (let [m2 (simple-monitor #(Thread/sleep 100) 1 1)]
    (is-= nil (execute m2))
    (is (nil? (execute m2)))
    (is (:too-slow (execute m2)))))

(def +elb-fuckoff-1+
  "Status Code: 503, AWS Service: AmazonElasticLoadBalancing, AWS Request ID: null, AWS Error Code: null, AWS Error Message: null")

(def +elb-fuckoff-2+
  "java.lang.RuntimeException: org.apache.http.conn.ConnectTimeoutException: Connect to elasticloadbalancing.amazonaws.com/elasticloadbalancing.amazonaws.com/176.32.100.121 timed out"
  )

(def +elb-fuckoff-3+
  [{:unhealthy-ring-member
    {:state "Unknown", :reasonCode "ELB", :instanceId "i-a1775dde"}}
   {:unhealthy-ring-member
    {:state "Unknown", :reasonCode "ELB", :instanceId "i-af7117d0"}}])

(def +real-error-1+
  "java.lang.RuntimeException: bla")

(def +real-error-2+
  [{:unhealthy-ring-member
    {:state "Unknown", :reasonCode "Shit", :instanceId "i-a1775dde"}}
   {:unhealthy-ring-member
    {:state "Unknown", :reasonCode "Shit", :instanceId "i-af7117d0"}}])

(deftest ring-alert-test
  (is (not (ring-alert? 'not-a-fn nil)))
  (is (not (ring-alert? 'not-a-fn [+real-error-1+])))
  (is (ring-alert? 'not-a-fn [+real-error-1+ +real-error-2+ +real-error-2+ +real-error-2+ +real-error-2+]))

  (is (not (ring-alert? (constantly true) (concat (repeat 3 +elb-fuckoff-1+)
                                                  (repeat 3 +elb-fuckoff-2+)
                                                  (repeat 3 +elb-fuckoff-3+)))))
  (is (ring-alert? (constantly false) (concat (repeat 3 +elb-fuckoff-1+)
                                              (repeat 3 +elb-fuckoff-2+)
                                              (repeat 3 +elb-fuckoff-3+))))
  (is (ring-alert? (constantly true) (concat (repeat 4 +elb-fuckoff-1+)
                                             (repeat 3 +elb-fuckoff-2+)
                                             (repeat 3 +elb-fuckoff-3+)))))

(deftest map-similarity-test
  (let [lhs {:a 3 :b 2 :c 19 :d -1}
        rhs {:a 2 :b 3 :d 12 :e 4 :f 8}
        nooverlap {:p 23 :q 44}
        within-epsilon (fn [epsilon] (fn [a b] (< (Math/abs (- a b)) epsilon )))]
    (is (= (map-similarity (within-epsilon 1.1) lhs rhs) (/ 2 5)))
    (is (= (map-similarity (within-epsilon 13.1) lhs rhs) (/ 3 5)))
    (is (= (map-similarity (within-epsilon 3) lhs nooverlap) 0.0))))

;; confirm that monitors compose correctly?
(defrecord CountingMockMonitor [execcounter reportcounter]
  Monitor
  (execute [this]
    (swap! execcounter inc))
  (daily-report [this]
    (swap! reportcounter inc)))

(deftest composite-monitor-test
  (let [a-exec (atom 0)
        b-exec (atom 0)
        a-report (atom 0)
        b-report (atom 0)
        monitor-a (CountingMockMonitor. a-exec a-report)
        monitor-b (CountingMockMonitor. b-exec b-report)
        composite (composite-monitor {:a monitor-a :b monitor-b})]
    (do
      (execute composite)
      (execute composite)
      (execute composite)
      (is (= @a-exec 3))
      (is (= @b-exec 3))
      (daily-report composite)
      (daily-report composite)
      (is (= @a-report 2))
      (is (= @b-report 2)))))
