(ns aws.kinesis
  "Low-level interface for publishing to kinesis.

   Recommended higher-level interfaces are in the separate kinesis project."
  (:use plumbing.core)
  (:require
   [clojure.set :as set]
   [schema.core :as s]
   [plumbing.logging :as log]
   [plumbing.serialize :as serialize]
   [aws.core :as aws])
  (:import
   [com.amazonaws.auth AWSCredentialsProvider]
   [com.amazonaws.services.kinesis AmazonKinesisClient]
   [com.amazonaws.services.kinesis.model Record GetRecordsRequest Shard GetShardIteratorRequest]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Management

(defnk kinesis :- AmazonKinesisClient
  [key secretkey]
  (doto (AmazonKinesisClient. (aws/credentials-provider key secretkey))
    (.setEndpoint "kinesis.us-east-1.amazonaws.com")))

(s/defn create!
  "Create a stream with n-shards.  See AWS documentation, but each shard can
   do roughly 1000 ops/second on 50k items. "
  [client :- AmazonKinesisClient
   stream-name :- String
   n-shards :- long]
  (.createStream client stream-name (int n-shards)))

(s/defn delete!
  [client :- AmazonKinesisClient
   stream-name :- String]
  (.deleteStream client stream-name))

(s/defn streams
  [client :- AmazonKinesisClient]
  (vec (.getStreamNames (.listStreams client))))

(defn shard->clj [^Shard r]
  {:id (.getShardId r)
   ;; TODO: other fields.
   })

(s/defn describe
  [client :- AmazonKinesisClient
   stream :- String]
  (let [res (.getStreamDescription (.describeStream client stream))]
    (assert (not (.getHasMoreShards res))) ;; paging not implemented
    {:status (.getStreamStatus res)
     :shards (mapv shard->clj (.getShards res))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Reading from streams.

;; Most production readers should use checkpointing with the kinesis client in kinesis-client
;; these simple methods are primarly for non-production uses and monitoring of production uses.

;; WARNING: these methods can throw ProvisionedThroughputExceptions

(defn ^Record ->record [^bytes b partition-key sequence-number]
  (-> (Record.)
      (.withData (java.nio.ByteBuffer/wrap b))
      (.withPartitionKey partition-key)
      (.withSequenceNumber sequence-number)))

(defn record->clj [^Record r]
  {:data (serialize/get-bytes (.getData r))
   :partition-key (.getPartitionKey r)
   :sequence-number (.getSequenceNumber r)})

(s/defn next-shard-page
  "Fetch the next page given response from first-shard-page or next-shard-page.
   See first-shard-page for details."
  [client :- AmazonKinesisClient
   limit :- long
   iterator :- (s/named String "Returned as the next-start of a previous call")]
  (let [res (.getRecords
             client
             (doto (GetRecordsRequest.)
               (.setLimit (int limit))
               (.setShardIterator iterator)))]
    [(mapv record->clj (.getRecords res))
     (.getNextShardIterator res)]))

(s/defn shard-iterator
  [client :- AmazonKinesisClient
   stream :- String
   shard :- String
   start :- (s/either (s/enum :oldest :newest) (s/named String 'sequence-number))]
  (.getShardIterator ;; ♫ one more time ♫
   (.getShardIterator
    client
    (-> (GetShardIteratorRequest.)
        (.withShardId shard)
        (.withStreamName stream)
        (.withShardIteratorType
         (case start
           :oldest "TRIM_HORIZON"
           :newest "LATEST"
           "AT_SEQUENCE_NUMBER"))
        (?> (string? start) (.withStartingSequenceNumber ^String start))))))

(s/defn first-shard-page
  "**WARNING**: Not for production usage.

   Return [batch next-start] of items from the shard, starting at the
   oldest result, results retrieved starting now, or a specific
   sequence number.  Not robust to thrown exceptions, batch-size must
   be set carefully to fall within transaction and size limits, and
   can throw ProvisionedThroughputExceptions at the drop of a hat.

   Shards can be obtained by calling describe on the stream.

   Call next-shard-page with next-start to get another batch."
  [client :- AmazonKinesisClient
   stream :- String
   shard :- String
   limit :- Long
   start :- (s/either (s/enum :oldest :newest) (s/named String 'sequence-number))]
  (next-shard-page client limit (shard-iterator client stream shard start)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Writing to streams

(s/defn put-record!
  "Write a single record to a stream under a given partition key.

   **WARNING** Item must be at most 50K, partitionKey must be evenly partitionable into
   enough shards, and may throw exceptions if over throughput limits.
   (str (UUID/randomUUID)) is a reasonable partition key if you don't know what to pass.

   To make sure readers are keeping up, it is customary to put a timestamp in the data."
  [client :- AmazonKinesisClient
   stream :- String
   partition-key :- String
   data :- bytes]
  (.putRecord client stream (java.nio.ByteBuffer/wrap data) partition-key))

(set! *warn-on-reflection* false)
