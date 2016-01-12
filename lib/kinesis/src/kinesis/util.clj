(ns kinesis.util
  "Utils for working with kinesis."
  (:use plumbing.core)
  (:require
   [plumbing.logging :as log]
   [kinesis.client :as client])
  (:import
   [com.amazonaws.services.kinesis.clientlibrary.interfaces IRecordProcessor]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: batched record processor

(defn count-record! [stats-counter decoded-record]
  (letk [[date messages] decoded-record]
    (stats-counter :input-messages (count messages))
    (stats-counter :input-latency-ms (double (- (millis) date)))))

(defn ^IRecordProcessor batched-record-processor
  "Simple record processor that batches messages up to a fixed size and flushes
   with the provided function.  The function should be synchronous, and will be
   retried indefinitely upon failure. (TODO: smarter policy about persistent
   failures).  Will flush a smaller batch upon stream termination.

   Guaranteed to keep all messages from a record together, and (unless a single record
   has more than batch-size messages), that each batch will have <= batch-size messages
   whose date range is at most max-date-range.

   Calls the write function with (write shard-id start-timestamp end-timestamp messages).
   start-ts_end-ts_shard-id makes a good batch key in s3.

   Guaranteed that every message will be written at least once, unless the subscriber
   falls behind the data retention limit of kinesis (currently 1 day), so this lag time
   should be monitored.  Logs information about message throughput and latency to the
   stats-counter for this purpose.

   Be aware that it's possible for some records to get saved multiple times, if a
   write goes through before it can be successfully checkpointed."
  [stats-counter batch-size max-date-range write-batch!]
  (let [q (atom nil)
        consume! (fn []
                   (when-let [data (get-and-set! q nil)]
                     (letk [[shard-id start-date end-date messages] data]
                       (loop []
                         (when (try (write-batch! shard-id start-date end-date messages)
                                    false
                                    (catch Throwable t
                                      (log/errorf t "Error writing kinesis message batch; retrying.")
                                      (Thread/sleep 100)
                                      true))
                           (recur))))))]
    (client/simple-record-processor
     (fn on-record [shard-id seq-num date msgs checkpoint]
       (when-let [data @q]
         (letk [[start-date messages last-seq-num] data]
           (when (or (> (+ (count messages) (count msgs)) batch-size)
                     (> (- date start-date) max-date-range))
             (consume!)
             (checkpoint last-seq-num))))
       (count-record! stats-counter {:date date :messages msgs})
       (swap! q (fn [old]
                  (if old
                    (-> old
                        (update :messages into msgs)
                        (assoc :end-date date
                               :last-seq-num seq-num))
                    {:shard-id shard-id
                     :start-date date
                     :end-date date
                     :messages (vec msgs)
                     :last-seq-num seq-num}))))
     (fn shutdown [checkpoint]
       (consume!)
       (checkpoint)))))

(set! *warn-on-reflection* false)
