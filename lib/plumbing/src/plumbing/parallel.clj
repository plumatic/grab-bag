(ns plumbing.parallel
  (:use plumbing.core)
  (:require
   [plumbing.error :as err]
   [plumbing.logging :as log]
   [plumbing.new-time :as new-time]
   [plumbing.observer :as observer]
   [plumbing.queue :as queue]
   [plumbing.resource :as resource])
  (:import
   [java.util Calendar Date]
   [java.util.concurrent ExecutorService Executors Future
    RejectedExecutionException
    RejectedExecutionHandler TimeoutException
    ScheduledExecutorService TimeUnit]
   [plumbing SaneExecutorService]))


(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Creating and destroying thread pools

(defn available-processors []
  (.availableProcessors (Runtime/getRuntime)))

;; maybe update to SaneExecutorService
(defn ^ExecutorService fixed-thread-pool [n-threads]
  (Executors/newFixedThreadPool n-threads))

(defn ^ExecutorService cached-thread-pool []
  (Executors/newCachedThreadPool))

(defn ^ScheduledExecutorService scheduled-thread-pool [& [n-threads]]
  (let [n (or n-threads 1)]
    (if (= n 1)
      (Executors/newSingleThreadScheduledExecutor)
      (Executors/newScheduledThreadPool n))))

(defn shutdown [^ExecutorService executor]
  (doto executor .shutdown))

;; TODO: should this be two-phase?
(extend-type ExecutorService
  resource/PCloseable
  (close [this] (shutdown this)))

(defn shutdown-now [^ExecutorService executor]
  (doto executor .shutdownNow))

(defn two-phase-shutdown
  "Shuts down an ExecutorService in two phases.
   First calls shutdown to reject incoming tasks, then calls shutdownNow.
   http://download-llnw.oracle.com/javase/6/docs/api/java/util/concurrent/ExecutorService.html"
  [^ExecutorService pool]
  (.shutdown pool)  ;; Disable new tasks from being submitted
  (err/with-ex ;; Wait a while for existing tasks to terminate
    (fn [e _ _]
      (when (instance? InterruptedException e)
        ;;(Re-)Cancel if current thread also interrupted
        (.shutdownNow pool)
        ;; Preserve interrupt status
        (.interrupt (Thread/currentThread))))
    #(if (not (.awaitTermination pool 60 TimeUnit/SECONDS))
       (.shutdownNow pool) ; // Cancel currently executing tasks
       ;;wait a while for tasks to respond to being cancelled
       (if (not (.awaitTermination pool 60 TimeUnit/SECONDS))
         (println "Pool did not terminate" *err*)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Scheduling and waiting for tasks

(defn schedule-work
  "schedules work. cron for clojure fns. Schedule a single fn with a pool to run every period-secs seconds."
  [f period-secs & [initial-offset]]
  (let []
    (doto (scheduled-thread-pool)
      (.scheduleAtFixedRate
       (partial err/with-ex (err/logger) f)
       (long (* 1000 (or initial-offset 0)))
       (long (* 1000 period-secs))
       TimeUnit/MILLISECONDS))))


(defn limited-future
  "Execute f on a future under executor e, and cancel/interrupt after limit-ms."
  [^ScheduledExecutorService s ^ExecutorService e limit-ms f]
  (let [fut-atom (atom nil)
        ^Runnable cancel #(.cancel ^java.util.concurrent.Future @fut-atom true)
        ^Runnable self-killing-f #(do (.schedule s cancel (long limit-ms) TimeUnit/MILLISECONDS) (f))
        fut (.submit e ^Runnable self-killing-f)]
    (reset! fut-atom fut)
    fut))

(defnk schedule-limited-work [f submit-rate-ms time-limit-ms {max-threads nil}]
  (let [work-pool (if max-threads (fixed-thread-pool max-threads) (cached-thread-pool))
        scheduled-pool (scheduled-thread-pool)]
    (.scheduleAtFixedRate
     scheduled-pool
     (fn [] (limited-future scheduled-pool work-pool time-limit-ms f))
     (long 0)
     (long submit-rate-ms)
     TimeUnit/MILLISECONDS)
    (reify
      resource/PCloseable
      (close [this]
        (shutdown work-pool)
        (shutdown scheduled-pool)))))

(defn wait-until
  ([cond-f]
     (wait-until cond-f Long/MAX_VALUE 1000))
  ([cond-f secs]
     (wait-until cond-f secs 1000))
  ([cond-f secs sleep-ms]
     (loop [secs-left secs]
       (cond
        (cond-f) true
        (neg? secs-left) false
        :else (do (Thread/sleep sleep-ms)
                  (recur (- secs-left (/ sleep-ms 1000))))))))

(defprotocol Refreshing
  (refresh! [this]))

(defn wrap-with-info
  "log info about start and end for infrequent scheduled works"
  [context f period-secs]
  (if (or (nil? period-secs) (< period-secs 3600))
    f
    (fn []
      (let [result-ref (atom nil)
            start-time (millis)]
        (log/infof "[START SCHEDULED at %s %s]" start-time context)
        (reset! result-ref (f))
        (log/infof "[END SCHEDULED started %s %s]" (new-time/pretty-ms-ago start-time) context)
        @result-ref))))

;; TODO: clean this shit up
(defnk scheduled-work [{observer nil} f period-secs {initial-offset-secs 0} {invoke-on-shutdown? false}]
  (let [context (when observer (observer/get-path observer))
        f (wrap-with-info context f period-secs)
        pool (schedule-work
              (fn [] (err/?error "Error in scheduled-work" (locking f (f))))
              period-secs
              initial-offset-secs)]
    (reify
      Refreshing
      (refresh! [this] (locking f (f)))
      resource/PCloseable
      (close [this]
        (shutdown pool)
        (when invoke-on-shutdown? (refresh! this))))))

(defnk daily-cron [f]
  (scheduled-work
   {:f f
    :period-secs (* 60 60 24)
    :initial-offset-secs (-> (doto ^Calendar (new-time/pst-calendar)
                                   ;; tomorrow at 12:05 AM
                                   (.set Calendar/HOUR_OF_DAY 0)
                                   (.set Calendar/MINUTE 5)
                                   (.set Calendar/SECOND 0)
                                   (.set Calendar/MILLISECOND 0)
                                   (.add Calendar/DAY_OF_MONTH 1))
                             ^Date (.getTime)
                             (.getTime)
                             (- (millis))
                             (new-time/from-millis :secs))}))

(defnk scheduled-work-now [f period-secs :as args]
  (f)
  (scheduled-work args))

(defnk refreshing-resource
  "Produce an dereffable object (also callable with no args) that provides a
   cached value of (f), updated every `secs` (unless f throws an exception or secs
   is nil).  The initial call is started immediately; if `block` is true the resource
   is not returned until the value is ready (and an exception in this first call
   to f will propagate upwards); otherwise, attempts to deref the resource will
   block until the value is ready (which may be forever if f always exceptions)."
  [{observer nil} f secs {block? false}]
  (let [context (when observer (observer/get-path observer))
        f (wrap-with-info context f secs)
        a (atom nil)
        set? (atom false)
        reload (fn []
                 (reset! a (f))
                 (reset! set? true))
        _ (when block? (reload))
        logging-reload (fn [] (err/?error "Error in refreshing-resource" (reload)))
        pool (if secs
               (schedule-work logging-reload secs (when block? secs))
               (future (logging-reload)))]
    (reify
      clojure.lang.IDeref
      (deref [this] (wait-until #(deref set?)) @a)
      clojure.lang.IFn
      (invoke [this] (wait-until #(deref set?)) @a)
      Refreshing
      (refresh! [this] (reload))
      resource/PCloseable
      (close [this] (when secs (shutdown pool))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Running tasks in parallel

(defn- java-deref [^Future f] (.get f))

(defn submit! [^ExecutorService pool ^Callable c]
  (.submit pool c))

(defn ^ExecutorService ensure-pool [pool-or-count-or-nil]
  (cond (instance? ExecutorService pool-or-count-or-nil) pool-or-count-or-nil
        (number? pool-or-count-or-nil) (fixed-thread-pool pool-or-count-or-nil)
        :else (fixed-thread-pool (+ 2 (available-processors)))))

(defmacro with-pool
  "creates a thread pool for use in the body, and shuts down the thread pool at the end of execution."
  [[pool-sym pool-or-count-or-nil] & body]
  `(let [pool-arg# ~pool-or-count-or-nil
         ~pool-sym (ensure-pool pool-arg#)]
     (try ~@body
          (finally (when-not (identical? pool-arg# ~pool-sym)
                     (shutdown ~pool-sym))))))



;; should return impl of protocol
;; or should return something which can be taken as arg to submit


(defn map-work
  "map using pool that returns a lazy seq of results in order, and attempts to
   cancel remaining computations as quickly as possible after an exception (so
   long as you are consuming the output sequence).
   first arg can be an ExecutorService, or a thread count, or nil (for 2x
   available processors).  any created pool will be shutdown automatically."
  ([f coll] (map-work nil f coll))
  ([pool-or-count-or-nil f coll]
     (when (seq coll)
       (if (= 1 pool-or-count-or-nil)
         (map f coll)
         (with-pool [pool pool-or-count-or-nil]
           (let [ex (atom nil)
                 process (fn [x] (submit! pool (fn [] (try (f x) (catch Throwable t (swap! ex #(or % t)) (throw t))))))
                 futs (doall (map process coll))]
             (concat
              (map #(if-let [e @ex]
                      (do (doseq [^Future f futs] (.cancel f true))
                          (throw e))
                      (java-deref %))
                   futs)
              (lazy-seq (when-let [e @ex] (throw e)))))))))
  ([batch-size pool-or-count-or-nil f coll]
     (->> coll
          (partition-all batch-size)
          (map-work pool-or-count-or-nil (partial mapv f))
          aconcat)))

(def keep-work (comp #(remove nil? %) map-work))

(defn do-work
  "do-work : map-work :: doseq : map"
  [& args]
  ;; make the fn return nil so we don't temporarily leak memory during the job.
  (dorun
   (apply map-work (update-in (vec args) [(- (count args) 2)] (fn [f] (fn [x] (f x) nil))))))

(defn map-vals-work [num-threads f m]
  (->> m
       (map-work
        num-threads
        (fn [[k v]]
          [k (f v)]))
       (into {})))

(defmacro letp
  "let where the binding expressions are executed in parallel using 'future'"
  [binding & body]
  (assert (even? (count binding)))
  (let [bps (partition 2 binding)
        bss (map (fn [_] (gensym)) bps)
        final-bindings (apply concat
                              (concat (map (fn [s [_ v]] [s `(future ~v)]) bss bps)
                                      (map (fn [s [b _]] [b `(deref ~s)]) bss bps)))]
    `(let ~(vec final-bindings) ~@body)))

(defmacro vectorp [& exprs]
  `(mapv deref ~(vec
                 (for [e exprs]
                   `(future ~e)))))

(defmacro dop [& exprs]
  `(dorun (vectorp ~@exprs)))

;;;; dealing with SaneExecutorService

;; on rejection from full queue, drops from front of queue
;; and runs callback on that popped item

(defn ^RejectedExecutionHandler callback-on-rejection-handler [callback]
  (reify RejectedExecutionHandler
    (rejectedExecution [this r executor]
      (let [queue (.getQueue executor)]
        (if (.isShutdown executor)
          (throw (RejectedExecutionException. "executor is shutdown already"))
          (do (callback (.poll queue))
              (.execute executor r)))
        )
      nil)))

;; valid queue-types :
;; fifo-unbounded
;; fifo-bounded-blocking
;; fifo-bounded-reject-newest
;; fifo-bounded-dump-oldest (runs callback on eldest item, popped from front)
;; priority
(defnk exec-service [num-threads
                     {queue-type :fifo-unbounded}
                     {bound nil}
                     {rejection-callback nil}]
  (let [queue (case queue-type
                :fifo-unbounded (do (assert (nil? bound))
                                    (queue/local-queue))
                (:fifo-bounded-blocking :fifo-bounded-dump-oldest :fifo-bounded-reject-newest)
                (do (assert bound)
                    (queue/blocking-queue bound))
                :priority (do (assert (nil? bound))
                              (queue/priority-queue)))
        reject-handler (case queue-type
                         (:fifo-unbounded :priority :fifo-bounded-reject-newest)
                         (do (assert (nil? rejection-callback))
                             (java.util.concurrent.ThreadPoolExecutor$AbortPolicy.))
                         (:fifo-bounded-blocking)
                         (do (assert (nil? rejection-callback))
                             (java.util.concurrent.ThreadPoolExecutor$CallerRunsPolicy.))
                         (:fifo-bounded-dump-oldest)
                         (do (assert rejection-callback)
                             (callback-on-rejection-handler rejection-callback)))]
    (SaneExecutorService. num-threads num-threads 1000 queue reject-handler)))

(defn submit-task
  ([es f] (submit-task es f {}))
  ([^SaneExecutorService es f options]
     (submit! es  (with-meta f (update-in options [:priority] (fnil double Double/NEGATIVE_INFINITY))))))

(defn clear-tasks [^SaneExecutorService es]
  (doseq [^Future t (.getQueue es)]
    (.cancel t false))
  (.purge es))

(defmacro with-timeout
  "Wait max-ms for body-expr to run in a future, else cancel the future and run fail-expr."
  [max-ms body-expr fail-expr]
  `(let [f# (future ~body-expr)]
     (try
       (.get f# ~max-ms TimeUnit/MILLISECONDS)
       (catch TimeoutException e#
         (future-cancel f#)
         (try
           (when-not (future-done? f#)
             (deref f#))
           (catch InterruptedException e2#))
         ~fail-expr))))

(defn throttle
  "Call f at most once every ms milliseconds, otherwise call g."
  [ms f g]
  (let [last-atom (atom 0)]
    (fn throttled [& args]
      (let [now (millis)
            last-t @last-atom]
        (if (or (< (- now last-t) ms)
                (not (compare-and-set! last-atom last-t now)))
          (apply g args)
          (apply f args))))))

(set! *warn-on-reflection* false)
