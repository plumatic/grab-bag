(ns monitoring.monitors.core-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.new-time :as new-time]
   [monitoring.monitors.core :as monitors]))


(deftest metric-threshold-test
  (let [thresh (fn [actual-age max-age data ranges & [allow-missing?]]
                 (with-millis (new-time/to-millis actual-age :mins)
                   (with-redefs [monitors/latest-metric (constantly data)]
                     (set (keys (monitors/metric-threshold nil "METRIC" max-age allow-missing? ranges))))))]
    (is (= #{} (thresh 8 10 {:date 0} {})))
    (is (= #{:stale-metric} (thresh 11 10 {:date 0} nil)))
    (is (= #{:missing-metric} (thresh 8 10 nil nil)))
    (is (= #{} (thresh 8 10 {:date 0 :foo 3} {:foo [1 5]})))
    (is (= #{} (thresh 8 10 {:date 0 :foo 3} {:foo [nil nil]})))
    (is (= #{:foo} (thresh 8 10 {:date 0} {:foo [1 5]})))
    (is (= #{} (thresh 8 10 {:date 0} {:foo [1 5]} true)))
    (is (= #{:foo} (thresh 8 10 {:date 0 :foo 0} {:foo [1 5]})))
    (is (= #{:foo} (thresh 8 10 {:date 0 :foo 7} {:foo [nil 5]})))
    (is (= #{:foo} (thresh 8 10 {:date 0 :foo 7 :bar 2} {:foo [nil 5] :bar [1 3]})))
    (is (= #{:foo :bar} (thresh 8 10 {:date 0 :foo 7} {:foo [nil 5] :bar [1 3]})))))
