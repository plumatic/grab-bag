(ns kinesis.util-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.core-incubator :as pci]
   [plumbing.serialize :as serialize]
   [aws.kinesis :as aws-kinesis]
   [kinesis.client :as client]
   [kinesis.core :as kinesis]
   [kinesis.util :as util]))

(deftest batched-record-processor-test
  (with-redefs [client/checkpoint! (recorder)]
    (let [stats (recorder)
          write (recorder)
          shard-id "SHARD"
          processor (util/batched-record-processor stats 5 1000 write)
          record (fn [date msgs seq-num]
                   (aws-kinesis/->record
                    (with-millis date
                      (pci/safe-singleton
                       (serialize/pack (kinesis/record-encoder) msgs)))
                    "PART"
                    (str seq-num)))]
      (.initialize processor shard-id)
      (testing "insert 2 1/2 batches of 2 records of 2 messages each"
        (doseq [recs (partition-all
                      2
                      (for [i (range 5)]
                        (record (* i 100) [(* 2 i) (inc (* 2 i))] i)))]
          (with-millis 1000
            (.processRecords processor recs nil)))
        (is-= [["SHARD" 0 100 [0 1 2 3]] ["SHARD" 200 300 [4 5 6 7]]] @write)
        (is-= [[nil "1"] [nil "3"]] @client/checkpoint!)
        (is-= (aconcat (for [l (range 1000.0 500.0 -100)]
                         [[:input-messages 2]
                          [:input-latency-ms l]]))
              @stats))
      (testing "flush preserve date range"
        (with-millis 10000
          (.processRecords processor [(record 5000 [10] 5)] nil))
        (is-= [["SHARD" 400 400 [8 9]]] @write)
        (is-= [[nil "4"]] @client/checkpoint!)
        (is-= [[:input-messages 1] [:input-latency-ms 5000.0]] @stats))
      (testing "shutdown flushes and checkpoints"
        (.shutdown processor nil com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason/TERMINATE)
        (is-= [["SHARD" 5000 5000 [10]]] @write)
        (is-= [[nil]] @client/checkpoint!)
        (is-= [] @stats)))))
