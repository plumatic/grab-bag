(ns plumbing.streaming-map
  (:use plumbing.core)
  (:require
   [plumbing.error :as err]
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.graph :as graph]
   [plumbing.graph-experimental :as graph-experimental]
   [plumbing.logging :as log]
   [plumbing.map :as map]
   [plumbing.new-time :as new-time]
   [plumbing.observer :as observer]
   [plumbing.parallel :as parallel])
  (:import
   [java.util.concurrent BlockingQueue ConcurrentLinkedQueue Future]
   [plumbing SaneExecutorService]))


(set! *warn-on-reflection* true)

(defprotocol PStreamingMap
  (submit [this input] "Submits input value to queue")
  (clear-tasks [this] "Clears current tasks in queue")
  (queue-size [this] "Returns number of items curently in queue")
  (report-queue-size [this] "Returns value to report to observer for current queue size(s)")
  (add-shutdown-step [this f] "Returns new version of this with additional shutdown hook"))

(defrecord PrioritizedItem [priority item callback])

(defrecord StreamingMap [f ^SaneExecutorService pool shutdown options]
  PStreamingMap
  (submit [this arg]
    (let [future (if (= (safe-get options :queue-type) :priority)
                   (do
                     (assert (instance? PrioritizedItem arg))
                     (parallel/submit-task pool #(f {:input (:item arg)})
                                           (select-keys arg [:priority :callback])))
                   (do
                     (assert (not (instance? PrioritizedItem arg)))
                     (parallel/submit-task pool #(f {:input arg}) {})))]
      (when (get options :timeout-tasks?)
        (let [^ConcurrentLinkedQueue running-tasks (:running-tasks this)]
          (.offer running-tasks [(millis) future])))))
  (clear-tasks [this]
    (parallel/clear-tasks pool))
  (queue-size [_]
    (.size ^BlockingQueue (.getQueue pool)))
  (report-queue-size [this]
    (queue-size this))
  (add-shutdown-step [this f]
    (update this :shutdown conj f))
  java.io.Closeable
  (close [this]
    (doseq [shutdown-step shutdown]
      (err/with-ex (err/logger) shutdown-step))))

(defrecord ShardedStreamingMap [f pools shutdown shard-val]
  PStreamingMap
  (submit [_ input]
    (let [num-pools (count pools)
          idx (or (some-> (shard-val input) hash (mod num-pools))
                  (rand-int num-pools))
          pool (nth pools idx)]
      (parallel/submit-task pool #(f input))))
  (clear-tasks [_]
    (doseq [pool pools]
      (parallel/clear-tasks pool)))
  (queue-size [_]
    (sum (for [^SaneExecutorService pool pools] (.size ^BlockingQueue (.getQueue pool)))))
  (report-queue-size [_]
    (for-map [[idx ^SaneExecutorService pool] (indexed pools)]
      (keyword (str idx))
      (.size ^BlockingQueue (.getQueue pool))))
  (add-shutdown-step [this f]
    (update this :shutdown conj f))
  java.io.Closeable
  (close [_]
    (doseq [shutdown-step shutdown]
      (err/with-ex (err/logger) shutdown-step))))

;; Graph stuff

(defmacro ordered-for-map [bind k-expr v-expr]
  `(->> (for ~bind [~k-expr ~v-expr])
        (apply concat)
        (apply array-map)))

(defn add-publishers [graph publishers]
  (graph/->graph
   (concat graph
           (for [[pub-from pub] publishers]
             [(keyword "plumbing.streaming-map" (str (name pub-from) "-pub"))
              (pfnk/fn->fnk
               (fn [m] (pub (safe-get m pub-from)))
               [{pub-from true} true])]))))

(defn with-node-name-exceptions [g]
  "Wrap the leaves of a graph with elaborated exceptions containing key path."
  (map/map-leaves-and-path
   (fn [ks f]
     (pfnk/fn->fnk
      (fn [m]
        (log/with-elaborated-exception {:node-keyseq ks}
          (f m)))
      (pfnk/io-schemata f)))
   g))

(defn observer-rewrite [g observer]
  (ordered-for-map
   [[k n] g]
   k
   (if (fn? n)
     (pfnk/fn->fnk
      (observer/observed-fn observer k {:type :stats} n)
      (pfnk/io-schemata n))
     (observer-rewrite n (observer/sub-observer observer k)))))

(defn log-wrap [f log-level]
  (fnk [input]
    (try (f {:input input})
         (catch Throwable t
           (log/log (get (ex-data t) :log-level log-level) t {:message "Exception in graph"})))))

;; Pool/queue stuff.

(defn streaming-map [f executor-map]
  (let [pool (parallel/exec-service executor-map)]
    (-> (->StreamingMap f pool [(fn [] (parallel/two-phase-shutdown pool))] executor-map)
        (?> (:timeout-tasks? executor-map)
            (assoc :running-tasks (ConcurrentLinkedQueue.))))))

(defn schedule-refill [sm refill-fn freq & [retain-queue?]]
  (let [refill-pool (parallel/schedule-work
                     (fn []
                       (when-let [tasks (seq (refill-fn (queue-size sm)))]
                         (when-not retain-queue? (clear-tasks sm))
                         (doseq [arg (remove nil? tasks)]
                           (submit sm arg))))
                     freq)]
    (add-shutdown-step sm (fn [] (parallel/two-phase-shutdown refill-pool)))))

(defn schedule-cancel-thread [{:keys [^ConcurrentLinkedQueue running-tasks] :as sm} cancel-after-seconds]
  ;; TODO support ShardedStreamingMap
  (let [cancel-pool (parallel/schedule-work
                     (fn []
                       (loop []
                         (let [[ts ^Future future] (.peek running-tasks)]
                           (when (and ts (< cancel-after-seconds (new-time/time-since ts :seconds)))
                             (when-not (.isDone future)
                               (.cancel future true))
                             (.poll running-tasks)
                             (recur)))))
                     ;; generally run n times (picked 4 arbitrarily) every timeout period
                     ;; and cancel any long running tasks.
                     (/ cancel-after-seconds 4))]
    (add-shutdown-step sm (fn [] (parallel/two-phase-shutdown cancel-pool)))))

(defn subscribe [sm subscribe-fn]
  (subscribe-fn #(submit sm %))
  sm)

(defn observe-queue [sm observer queues-key]
  (observer/report-hook
   observer (or queues-key (observer/gen-key "queues"))
   (fn []
     (report-queue-size sm)))
  sm)

;; note: all publishing happens after all processing...
;; TODO: way to kill only one branch of a graph (using killing compile?)
(defnk streaming-graph
  "Executes a graph in a thread pool on values from a queue.

   The type and behavior of the queue can be controlled by passing observe-queues?,
   bound, queue-type, and rejection-callback.

   If passed, refill-fn is called every refill-period seconds with the current queue size.
   If it returns a non-empty seq of items, the queue is cleared and all non-nil items
   are added to the queue.

   The queue can also be attached to a subscriber by passing the subscriber, and
   results from nodes can be published by passing a :publishers map from node keys
   to publishers.

   Users can also specify a timeout, in which case the streaming map will make a best
   effort to cancel tasks which run for longer than the timeout"
  [observer graph
   {num-threads 1}
   {refill-fn nil} {refill-period (* 60 60)}
   {retain-queue-on-refill? false}
   {publishers {}} {subscriber nil}
   {default-log-level :error}
   {observe-queues? false}
   {timeout-seconds nil}
   {bound nil}
   {queue-type :fifo-unbounded}
   {rejection-callback nil}]
  (-> graph
      (add-publishers publishers)
      (observer-rewrite observer)
      (graph-experimental/eager-clearing-compile [])
      (log-wrap default-log-level)
      (streaming-map {:num-threads num-threads :queue-type queue-type
                      :bound bound :rejection-callback rejection-callback
                      :timeout-tasks? (boolean timeout-seconds)})
      (?> observe-queues? (observe-queue observer :queues))
      (?> timeout-seconds (schedule-cancel-thread timeout-seconds))
      (?> refill-fn (schedule-refill refill-fn refill-period retain-queue-on-refill?))
      (?> subscriber (subscribe subscriber))))

(def streaming-fn
  (graph/instance streaming-graph [f {publisher nil}]
    {:graph {:sole-node (fnk [input] (f input))}
     :publishers (when publisher {:sole-node publisher})}))

(defnk sharded-streaming-fn
  "Streams messages into f using a multiple threads. Each thread gets own
   queue and input can be consistently routed to the same queue.

   Routing formula: queue-id = (mod (shard-val input) num-threads)

   If shard-val returns nil, input will be submitted to random queue.

   See plumbing.parallel/exec-service for queue-type and bound options."
  [f num-threads observer
   {shard-val (constantly nil)}
   {refill-fn nil} {refill-period (* 60 60)}
   {retain-queue-on-refill? false}
   {subscriber nil}
   {queue-type :fifo-unbounded}
   {bound nil}
   {rejection-callback nil}
   {observe-queues? false}]
  (let [pools (vec (for [_ (range num-threads)]
                     (parallel/exec-service
                      {:num-threads 1
                       :queue-type queue-type
                       :bound bound
                       :rejection-callback rejection-callback})))
        shutdown (mapv (fn [pool] #(parallel/two-phase-shutdown pool)) pools)]
    (-> (ShardedStreamingMap. f pools shutdown shard-val)
        (?> observe-queues? (observe-queue observer :queues))
        (?> refill-fn (schedule-refill refill-fn refill-period retain-queue-on-refill?))
        (?> subscriber (subscribe subscriber)))))
