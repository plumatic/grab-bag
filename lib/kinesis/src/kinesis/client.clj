(ns kinesis.client
  "Client for reading from kinesis"
  (:use plumbing.core)
  (:require
   [plumbing.error :as err]
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [plumbing.new-time :as new-time]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [plumbing.serialize :as serialize]
   [aws.core :as aws]
   [kinesis.core :as kinesis])
  (:import
   [com.amazonaws.services.kinesis.clientlibrary.interfaces IRecordProcessor
    IRecordProcessorCheckpointer
    IRecordProcessorFactory]
   [com.amazonaws.services.kinesis.clientlibrary.lib.worker KinesisClientLibConfiguration
    Worker]
   [com.amazonaws.services.kinesis.clientlibrary.types ShutdownReason]
   [com.amazonaws.services.kinesis.model Record]))

(set! *warn-on-reflection* true)

(extend-type Worker
  resource/PCloseable
  (close [this] (.shutdown this)))

(def +max-tries+ 5)

(defn checkpoint! [^IRecordProcessorCheckpointer checkpointer maybe-last-sequence-num]
  (err/with-retries
    +max-tries+ 1000 (fn [e] (log/warnf e "Failed to checkpoint!"))
    (if maybe-last-sequence-num
      (.checkpoint checkpointer maybe-last-sequence-num)
      (.checkpoint checkpointer))))

(defn ^IRecordProcessor simple-record-processor
  "On-record is called on (on-records shard-id sequence-num timestamp
   messages checkpointer).  Checkpointer is a function of sequence
   number, and should be called on a sequence number when all lower
   numbers (from previous calls) have been processed.  On-shutdown is
   a function that processes all queued records and then checkpoints
   (with no args)."
  [on-record on-shutdown]
  (let [shard-id-atom (atom nil)]
    (reify IRecordProcessor
      (initialize [this shard-id] (reset! shard-id-atom shard-id))
      (processRecords [this records checkpointer]
        (err/?error
         "Error processing record batch."
         (doseq [^Record record records]
           (letk [[date messages] (kinesis/decode-record
                                   (serialize/get-bytes
                                    (.getData record)))]
             (err/?error
              (format "Error in callback. Could not process: %s" record)
              (on-record
               @shard-id-atom
               (.getSequenceNumber record)
               date
               messages
               #(checkpoint! checkpointer %)))))))
      (shutdown [this checkpointer reason]
        (err/?error
         "Error running on-shutdown"
         (when (= reason ShutdownReason/TERMINATE)
           (on-shutdown #(checkpoint! checkpointer))))))))

(def subscriber-graph
  "Subscriber graph. Provide an app-name which scopes recovery,
   stream, and make-record-processor fn that returns an
   IRecordProcessor.  This record processor is responsible for
   processing records and checkpointing periodically."
  {:client-config (fnk [env app-name stream [:instance service-name] [:ec2-keys key secretkey]]
                    (KinesisClientLibConfiguration.
                     (str "kinesis-" app-name "-" (name env))
                     (kinesis/env-stream env stream)
                     (aws/credentials-provider key secretkey)
                     (str service-name ":" (java.util.UUID/randomUUID))))

   :worker (fnk [client-config make-record-processor]
             ;; TODO: shut down and flush record processors on restart?
             ;; it seems like KCL doesn't call shutdown on worker shutdown by default.
             (Worker.
              (reify IRecordProcessorFactory
                (createProcessor [this] (make-record-processor)))
              client-config))

   :run-worker (graph/instance parallel/scheduled-work [^Worker worker]
                 {:period-secs 1.0
                  :f (fn []
                       (log/infof "(re)starting worker!")
                       (.run worker))})})

(set! *warn-on-reflection* false)
