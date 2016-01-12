(ns plumbing.accumulators-test
  (:use plumbing.core clojure.test plumbing.accumulators plumbing.test)
  (:require [plumbing.logging :as log]
            [plumbing.parallel :as parallel]
            [plumbing.resource :as resource]))

(deftest ^:flaky time-batching-graph-test
  (let [batches (atom [])]
    (with-open [g (time-batching-fn {:secs 0.01 :f (partial swap! batches conj)})]
      (Thread/sleep 1)
      (dotimes [_ 100]
        (offer-to g 42))
      (is (parallel/wait-until #(= [100] (map count @batches)) 1 10)))))

(deftest time-batching-graph-overload-test
  (let [batches (atom [])]
    (with-open [g (time-batching-fn {:secs 0.01 :f (partial swap! batches conj)
                                     :max-queue-size 10})]
      (Thread/sleep 1)
      (dotimes [i 100]
        (offer-to g i))
      (is-eventually (seq @batches))
      (let [first-batch (first @batches)
            n (count first-batch)]
        (is (<= n 10))
        (is (= first-batch (range n)))))))


(deftest ^:slow too-slow-test
  (let [captured-last-run  (atom {:last-start-time (millis)
                                  :last-run-time 0})
        captured-runs (atom [])
        received (atom [])
        examinable-spec (assoc time-batching-graph
                          :last-drain-and-duration (fnk [] captured-last-run)
                          :when-slow (fnk []
                                       (fn [secs-since-last expected-secs last-duration queue-size]
                                         (swap! captured-runs conj [secs-since-last expected-secs last-duration queue-size]))))]
    (with-open [g ((resource/bundle-compile examinable-spec)
                   {:secs 0.001 ;; 10 ms
                    :f (fn [x]
                         (Thread/sleep (last x))
                         (log/infof  "x =>  %s, slept for %s" x (last x))
                         (swap! received conj x))
                    :min-slow-secs 0.0000001
                    :max-queue-size 5})]
      ;; quick batch; shouldn't touch captured-runs
      (doseq [i [2 3 2 1 2]]
        (offer-to g i))
      (Thread/sleep 20)
      (is-= 0 (count @captured-runs))
      (is-= @received ['( 2 3 2 1 2)])
      ;; slow run; should trigger 'when-slow' condition
      (doseq [i [2 3 2 1 15]]
        (offer-to g i))
      (Thread/sleep 30)
      (is-= 1 (count @captured-runs))
      (is-= [2 3 2 1 15] (second @received)))))
