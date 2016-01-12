(ns kinesis.publisher
  "Namespace for anyc publishing to kinesis."
  (:use plumbing.core)
  (:require
   [plumbing.error :as err]
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [plumbing.streaming-map :as streaming-map]
   [aws.kinesis :as aws-kinesis]
   [kinesis.core :as kinesis]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private helpers

(defn rand-str []
  (str (java.util.UUID/randomUUID)))

(defn safe-put-record!
  [client stream record]
  (err/with-retries 5 1000 #(throw %)
    (aws-kinesis/put-record! client stream (rand-str) record)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(def publisher-graph
  (graph/graph
   :kinesis-client (fnk [ec2-keys] (aws-kinesis/kinesis ec2-keys))
   :encoder (fnk [] (kinesis/record-encoder))
   :publish! (fnk [env kinesis-client stream]
               (if (= env :test)
                 (let [a (atom [])]
                   (fn test-publish
                     ([] (get-and-set! a []))
                     ([record] (swap! a conj record))))
                 (let [enved-stream (kinesis/env-stream env stream)]
                   (fn kinesis-publish [record]
                     (safe-put-record! kinesis-client enved-stream record)))))
   :streaming-map (graph/instance streaming-map/streaming-fn
                      [num-threads encoder publish!]
                    {:num-threads num-threads
                     :observe-queues? true
                     :queue-type :fifo-bounded-dump-oldest
                     :bound 100000
                     :rejection-callback (fn [msg]
                                           (log/errorf "Queue overflowing, dropping %s" msg))
                     :f (fn [m] (doseq [r (encoder m)] (publish! r)))})
   :periodic-flush (graph/instance parallel/scheduled-work
                       [encoder publish!]
                     {:period-secs 1
                      :f (fn [] (doseq [r (encoder)] (publish! r)))})
   :clean-shutdown (fnk [streaming-map periodic-flush]
                     (reify resource/PCloseable
                       (close [this]
                         (log/infof "Waiting for %s records to clear from publisher"
                                    (streaming-map/queue-size streaming-map))
                         (parallel/wait-until #(zero? (streaming-map/queue-size streaming-map)) 60 10)
                         (parallel/refresh! periodic-flush))))))

(defn submit! [g message]
  (streaming-map/submit (safe-get g :streaming-map) message))
