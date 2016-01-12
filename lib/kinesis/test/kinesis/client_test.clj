(ns kinesis.client-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [plumbing.auth :as auth]
   [plumbing.resource :as resource]
   [crane.config :as config]
   [aws.kinesis :as aws-kinesis]
   [kinesis.client :as client]
   [kinesis.publisher :as publisher]
   [kinesis.core :as kinesis]))

(defn test-kinesis
  "Test kinesis against AWS"
  [ec2-keys]
  (let [c (aws-kinesis/kinesis ec2-keys)
        msg (fn [i] (let [b (byte-array 100)]
                      (.nextBytes (java.util.Random. i) ^bytes b)
                      b))
        raw-stream (str "GRABBAG_TEST_STREAM" (rand-int 100))
        stream (str raw-stream "-dev")
        args {:env :dev
              :ec2-keys ec2-keys
              :observer nil
              :stream raw-stream}
        msgs-atom (atom [])
        junk (vec (repeatedly 10 #(auth/rand-str 10000))) ;; incompressible.
        msg (fn [payload] [payload (nth junk (mod payload 10))])]
    (try
      (aws-kinesis/create! c stream 1)
      (is-eventually (= "ACTIVE" (:status (aws-kinesis/describe c stream))) 60 1000)
      (resource/with-open [sub (resource/bundle-run
                                client/subscriber-graph
                                (assoc args
                                  :app-name (str "TEST" (rand-int 10))
                                  :instance {:service-name "testy-service"}
                                  :make-record-processor (fn []
                                                           (client/simple-record-processor
                                                            (fn process [shard-id seq-num date msgs cpt]
                                                              (swap! msgs-atom into (map first msgs))
                                                              (cpt seq-num))
                                                            (fn shutdown [f] (f))))))]
        (Thread/sleep 60000)
        (resource/with-open [pub (resource/bundle-run
                                  publisher/publisher-graph
                                  (assoc args
                                    :num-threads 1))]
          (dotimes [i 5]
            (publisher/submit! pub (msg i)))
          (is-eventually (= @msgs-atom [0 1 2 3]) 200 10))
        (is-eventually (= @msgs-atom [0 1 2 3 4])))
      (finally
        (aws-kinesis/delete! c stream)))))
