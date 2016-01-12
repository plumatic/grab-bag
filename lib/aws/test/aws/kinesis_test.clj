(ns aws.kinesis-test
  (:use plumbing.core plumbing.test clojure.test)
  (:require
   [aws.kinesis :as kinesis]))

(defn records [c stream shard batch-size start]
  (loop [[page next-start] (kinesis/first-shard-page c stream shard batch-size start) res []]
    (if (empty? page)
      res
      (recur (kinesis/next-shard-page c batch-size next-start) (into res page)))))

(defn test-kinesis
  "Run kinesis client through a few simple tests on a test stream, then destroy it."
  [ec2-creds]
  (let [c (kinesis/kinesis ec2-creds)
        msg (fn [i] (let [b (byte-array 100)]
                      (.nextBytes (java.util.Random. i) ^bytes b)
                      b))
        stream (str "GRABBAG_TEST_STREAM" (rand-int 100))]
    (try
      (kinesis/create! c stream 1)
      (is-eventually (= "ACTIVE" (:status (kinesis/describe c stream))) 60 1000)
      (is (some #{stream} (kinesis/streams c)))
      (dotimes [i 10]
        (kinesis/put-record! c stream (str i) (msg i)))
      (let [shards (safe-get (kinesis/describe c stream) :shards)
            shard (safe-get (first shards) :id)
            ->data (fn-> (dissoc :sequence-number) (update :data vec))]
        (is-= 1 (count shards))
        (is-=-by set
                 (for [i (range 10)]
                   {:data (vec (msg i))
                    :partition-key (str i)})
                 (map ->data (records c stream shard 5 :oldest)))
        (let [it2 (kinesis/shard-iterator c stream shard :newest)
              [p next] (kinesis/next-shard-page c 10 it2)]
          (is (empty? p))
          (kinesis/put-record! c stream (str 11) (msg 11))
          (let [[p] (kinesis/next-shard-page c 10 next)]
            (is-= [{:data (vec (msg 11))
                    :partition-key "11"}]
                  (map ->data p)))))
      (finally
        (kinesis/delete! c stream)))))
