(ns plumbing.accumulators
  (:use plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.queue :as queue]
   [plumbing.resource :as resource]
   [plumbing.timing :as timing])
  (:import
   [java.util.concurrent BlockingQueue]))

(defn run-batch! [queue f on-error]
  (try (when-let [s (seq (queue/drain queue))]
         (f s))
       (catch Throwable t
         (if on-error (on-error t)
             (log/errorf t "Error in time-batching-fn")))))

(def time-batching-graph
  (graph/graph
   :queue (fnk [{max-queue-size nil}]
            (if max-queue-size (queue/blocking-queue max-queue-size) (queue/local-queue)))
   :last-drain-and-duration (fnk [] (atom {:last-start-time (millis)
                                           :last-run-time 0}))

   ;; pluggability allows tests to confirm that we react to slow stuff appropriately
   :when-slow (fnk [] (fn [secs-since-last expected-secs last-duration queue-size]
                        (log/errorf "Time-Batching fn is falling behind: last run started %s seconds ago (expected %s), took %s millis. queue count %s"
                                    secs-since-last expected-secs last-duration queue-size)))
   :drain-pool (fnk [queue last-drain-and-duration f secs when-slow
                     {on-error nil} {min-slow-secs 90}]
                 (parallel/schedule-work
                  (fn []
                    (let [now (millis)
                          {:keys [last-start-time last-run-time]}  @last-drain-and-duration]
                      (when (> now (+ last-start-time (max (* 1000 min-slow-secs)
                                                           (* 1000 secs 10))))
                        (when-slow
                         (/ (- now last-start-time) 1000.0)
                         secs last-run-time (.size ^BlockingQueue queue)))
                      (let [last-run-time (timing/get-time (run-batch! queue f on-error))]
                        (reset! last-drain-and-duration {:last-start-time now
                                                         :last-run-time last-run-time}))))
                  secs))))

(def time-batching-fn (resource/bundle-compile time-batching-graph))

(defn offer-to [batch-graph x]
  (queue/offer (safe-get batch-graph :queue) x))
