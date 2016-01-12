(ns plumbing.observer
  (:use plumbing.core)
  (:require
   [plumbing.error :as err]
   [plumbing.logging :as log]
   [plumbing.map :as map]
   [plumbing.time :as time])
  (:import
   [java.lang.ref WeakReference]
   [java.util Timer TimerTask]
   [java.util.concurrent ExecutorService Executors]))

;; Observer Interface

(def +stop-watching+ (Object.))

(defprotocol Observer
  (observed-fn [this k opts f]
    "Return a wrapped version of f that logs results under k.
     Opts is either :log or :stats for now.")
  (watch-fn!* [this k merge-spec f args]
    "For internal use.  Users should call watch-fn!.")
  (leaf* [this k merge-spec]
    "Return a fn to call with observations, or nil.
     merge-spec is {:report :merge}, with defaults identity/conj
     merge-fn tells how to merge in new values, and report-fn generates a report from
     map plus an additional map of global values.")
  (sub-observer [this k]
    "Create a new sub-observer of obs under key k.")
  (report [this global-data]
    "Generate a report.")
  (get-path [this]
    "Return the seq of keys corresponding to sub-observer paths to this"))

(defn watch-fn!
  "Call f regularly, logging the results into k on obs.
   f can return +stop-watching+ to stop watching it, or it will be
   automatically stopped if any of the resources (passed as args)
   become collectable by the GC."
  [obs k merge-spec f & args]
  (watch-fn!* obs k merge-spec f args))

(defn report-hook "Call f on every report, generating a map reported under k"
  [observer k f]
  (leaf* observer k {:report (fn [_ _] (f))}))

(defn counter
  "Make a counter you can call with any number of keys, which counts into a nested map.
   Expect pain if you call on a strict prefix of keys, e.g., (c :kittens) (c :kittens :chin)"
  [o k]
  (let [raw
        (leaf*
         o k
         {:merge
          (fn [old ks]
            (update-in (or old {}) ks (fnil inc 0)))})]
    (fn [& ks] (when raw (raw ks)))))

(defn stats-counter
  "Like counter, but the last argument is a real number, and we'll accumulate the
   mean as well as the count."
  [o k]
  (let [raw
        (leaf*
         o k
         {:merge
          (fn [old [ks v]]
            (update-in (or old {}) ks (fn [[c t]] [(inc (or c 0)) (+ (or t 0.0) v)])))
          :report (fn [m _] (map/map-leaves (fn [[c t]] {:count c :mean (/ t c) :total t}) m))})]
    (fn [& ks] (when raw (raw [(drop-last ks) (last ks)])))))

(defn gen-key [^String prefix]
  (-> prefix gensym name keyword))


;;; Helpers for implementors

(defn watch-with-resources! [schedule! store! f res]
  (schedule!
   (let [refs (doall (map #(WeakReference. %) res))]
     (fn wrap []
       (let [res (map #(.get ^WeakReference %) refs)]
         (when (every? identity res)
           (let [result (apply f res)]
             (when-not (identical? result +stop-watching+)
               (store! result)
               (schedule! wrap)))))))))

(defn observe-fn-log [o k f]
  (if-let [l (leaf* o k {})]
    (comp l f)
    (constantly nil)))


(defn div [x y]
  (when (and x y (> y 0))
    (float (/ x y))))

(defn time-reporter []
  (let [start (millis)]
    (fn []
      (let [end (millis)]
        {:last end
         :time (max 0 (- end start))}))))

(defn observe-fn-stats [o k f opts]
  (if-let [l (leaf*
              o k
              {:merge (fn [old new]
                        (reduce (fn [m [k v]]
                                  (assoc m k (cons v (get m k))))
                                (or old {}) new))
               :report
               (fn [{:keys [time last] :as b} {:keys [duration]}]
                 (let [c (count time)
                       time (reduce + time)]
                   (assoc (map-vals count b)
                     :time       time
                     :last       (time/to-string (time/from-long (first last)))
                     :count      c
                     :avg-time   (div time c)
                     :p-data-loss (div (count (apply concat (vals (dissoc b :time :last)))) c))))})]
    (fn [& args]
      (let [time-report (time-reporter)
            result (try (apply f args)
                        (catch Throwable t
                          (l (assoc (time-report) (-> t err/cause err/ex-name) 1))
                          (throw t)))]
        (l (time-report))
        result))
    f))

(defn observe-fn-counts [obs k f group-fn]
  (if-let [l (leaf* obs k {:merge (partial merge-with +)
                           :report (fn [m _]
                                     (reduce (fn [m [ks v]] (assoc-in m ks v)) {} m))
                           })]
    (fn [& args]
      (l {(group-fn args) 1})
      (apply f args))
    f))

(defn default-observed-fn [obs k opts f]
  (let [opts (if (map? opts) opts {:type opts})]
    (case (:type opts)
      :log (observe-fn-log obs k f)
      :stats (observe-fn-stats obs k f opts)
      :counts (observe-fn-counts obs k f (:group opts)))))


;;; Noop implementation

(extend-type nil
  Observer
  (observed-fn [this k opts f] (default-observed-fn this k opts f))
  (watch-fn!* [this k merge-spec f rs] nil)
  (leaf* [this k merge-spec] nil)
  (sub-observer [this k] nil)
  (report [this global-data] nil)
  (get-path [this] []))


;;; Simple implementation --- stores under seqs of keys
;;; update-key! takes a key-seq and update-fn.

(defrecord SimpleObserver [key-seq update! get schedule! report-atom child-atom]
  Observer
  (observed-fn [this k opts f]
    (default-observed-fn this k opts f))
  (watch-fn!* [this k merge-spec f rs]
    (when-let [l (leaf* this k merge-spec)]
      (watch-with-resources! schedule! l f rs)))
  (leaf* [this k merge-spec]
    (let [{:keys [merge report]
           :or {merge conj report (fn [m _] m)}} merge-spec
           ks (conj key-seq k)]
      (when (:report merge-spec) ;; TODO: remove this when log is fixed.
        (assert (not (contains? @report-atom k))))
      (swap! report-atom assoc k #(report (get ks) %))
      (fn [x] (update! ks #(merge % x)))))
  (sub-observer [this k]
    (or (@child-atom k)
        (let [sub (SimpleObserver. (conj key-seq k) update! get schedule! (atom {}) (atom {}))]
          (swap! child-atom assoc k sub)
          (swap! report-atom assoc k #(report sub %))
          sub)))
  (report [this global-data]
    (map-vals #(% global-data) @report-atom))
  (get-path [this] key-seq))


(defn simple-scheduler []
  {:executor (Executors/newCachedThreadPool)
   :timer    (Timer.)})

(defn schedule-fn [{:keys [^ExecutorService executor ^Timer timer]} f in-ms]
  (.schedule
   timer
   (proxy [TimerTask] [] (run [] (.submit executor ^Runnable f)))
   (long in-ms)))

(defn make-simple-observer [update! get & [poll-delay]]
  (let [scheduler (simple-scheduler)
        poll-delay (or poll-delay 1000)]
    (SimpleObserver.
     []
     update! get
     #(schedule-fn scheduler % poll-delay)
     (atom {}) (atom {}))))

(defn make-atom-observer [& [poll-delay]]
  (let [a (atom {})]
    (make-simple-observer
     (fn [ks f] (swap! a update-in ks f))
     (fn [ks]
       (let [vs (get-in @a ks)]
         (swap! a update-in ks (constantly nil))
         vs))
     poll-delay)))
