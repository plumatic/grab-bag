(ns service.observer
  (:use plumbing.core [plumbing.error :only [logger with-ex]])
  (:require
   [schema.core :as s]
   [plumbing.error :as err]
   [plumbing.logging :as log]
   [plumbing.map :as map]
   [plumbing.math :as math]
   [plumbing.new-time :as new-time]
   [plumbing.observer :as plumbing-observer]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [store.bucket :as bucket]
   [store.mongo :as mongo]
   [store.snapshots :as snapshots]))


(set! *warn-on-reflection* true)

(defprotocol PersistentObserver
  (metric-logger [this name meta] "Serivce-name prepended"))

(defn rel-day [^long pst-offset]
  (.format (doto (java.text.SimpleDateFormat. "yyyyMMdd") (.setCalendar (java.util.Calendar/getInstance (java.util.TimeZone/getTimeZone "PST")))) (java.util.Date. (+ pst-offset (.getTime (java.util.Date.))))))

(defn today [] (rel-day 0))

(def +agg-metrics-coll+
  "Collection for storing all aggregate metrics across all services, as displayed on
   http://dashboard.example.com/admin/metrics-table."
  "agg-metrics")

(defn persistent-observer [lds service-name]
  (let [meta-bucket (mongo/bucket-collection lds "meta")
        agg-metrics (mongo/aggregate-collection
                     lds +agg-metrics-coll+
                     {:agg-keys [:day :service :name :key]})]
    (reify PersistentObserver
      (metric-logger [_ name meta]
        (assert (not-any? meta [:name :type]))
        (let [full-name (str service-name "-" name)
              meta (merge {:max-mb 100} meta)
              data-coll (mongo/capped-collection
                         lds full-name
                         (select-keys meta [:max-mb :max-count]))]
          (bucket/put meta-bucket full-name (assoc meta :name name :full-name full-name :type :metric :service service-name))
          (fn [datum]
            (let [datum (cond (map? datum) datum (nil? datum) {} :else {:value datum})
                  now (millis)
                  day (today)]
              (mongo/append data-coll (assoc datum :date (millis)))
              (doseq [[k v] datum]
                (mongo/update
                 agg-metrics
                 {:day day :service service-name :name name :key k}
                 {}
                 {:sum v :count 1})))))))))

(defn drain-to-map! [b]
  (into {}
        (for [k (bucket/keys b)]
          [k (bucket/delete b k)])))

(defn start-flushing! [obs-bucket snapshots report-fn flush-freq]
  (let [start (atom (millis))
        snap-count (atom 0)]
    (parallel/schedule-work
     (partial with-ex (logger)
              #(let [drained (drain-to-map! obs-bucket)]
                 (let [dur (new-time/time-since @start :secs)]
                   (snapshots/write-snapshot
                    snapshots @snap-count
                    (report-fn drained {:duration dur}))
                   (swap! snap-count inc)
                   (reset! start (millis)))))
     flush-freq)))

(defn free->used [free max]
  {:used (- max free)
   :max max})

(defn gb [x] (/ x 1073741824.0))

;; intentionally allow reflection so it compiles under Java 8.
(set! *warn-on-reflection* false)

(let [a (atom nil)]
  (defn process-load
    "Deprecated, used because pre-Java-8 UnixOperatingSystem doesn't have a jvm load."
    [b]
    (let [n (System/nanoTime)
          l (.getProcessCpuTime b)
          [pl pn] @a
          v (if pn (/ (- l pl) (- n pn -1.0)) 0.0)]
      (reset! a [l n])
      v)))

(defn observe-machine-old
  "Deprecated, used because pre-Java-8 UnixOperatingSystem doesn't work with bean."
  []
  (let [b (java.lang.management.ManagementFactory/getOperatingSystemMXBean)]
    (err/?debug "Failed to observe OS metrics"
                {:os {:arch (.getArch b)
                      :name (.getName b)
                      :version (.getVersion b)}
                 :cpu {:used (.getSystemLoadAverage b)
                       :max (.getAvailableProcessors b)}
                 :ram-plus-cache (free->used (gb (.getFreePhysicalMemorySize b)) (gb (.getTotalPhysicalMemorySize b)))
                 :file-descriptors {:used (.getOpenFileDescriptorCount b),
                                    :max  (.getMaxFileDescriptorCount b)}
                 :swap (free->used (gb (.getFreeSwapSpaceSize b)) (gb (.getTotalSwapSpaceSize b)))
                 :jvm {:cpu {:used (process-load b)
                             :max (.getAvailableProcessors b)}}})))

(set! *warn-on-reflection* true)

(defn observe-machine
  "Return information about the JVM and machine.  The bean approach works in Java 8, but fails
   on java 7, and vice-versa (Java 8 can't refer to UnixOperatingSystem) so we're
   stuck with this hack for now."
  []
  (try
    (let [{:keys [arch name version systemCpuLoad availableProcessors
                  totalPhysicalMemorySize freePhysicalMemorySize
                  openFileDescriptorCount maxFileDescriptorCount
                  freeSwapSpaceSize totalSwapSpaceSize
                  processCpuLoad]}
          (bean (java.lang.management.ManagementFactory/getOperatingSystemMXBean))]
      {:os {:arch arch
            :name name
            :version version}
       :cpu {:used systemCpuLoad
             :max availableProcessors}
       :ram-plus-cache (free->used (gb freePhysicalMemorySize) (gb totalPhysicalMemorySize))
       :file-descriptors {:used openFileDescriptorCount
                          :max  maxFileDescriptorCount}
       :swap (free->used (gb freeSwapSpaceSize) (gb totalSwapSpaceSize))
       :jvm {:cpu {:used processCpuLoad
                   :max availableProcessors}}})
    (catch IllegalAccessException e
      (observe-machine-old))))

(defn meminfo []
  (with-open [o (java.io.FileInputStream. "/proc/meminfo")]
    (slurp o))) ;; for some reason raw slurp fails

(defn meminfo-map []
  (into {}
        (for [row (.split ^String (meminfo) "\\n")]
          (let [[[_ k v]] (re-seq #"(.*): *(\d+).*" row)]
            [(keyword k) (Long/parseLong v)]))))

(defn observe-meminfo []
  (err/?debug "Failed to observe linux memory info"
              (letk [[MemTotal MemFree Buffers Cached] (meminfo-map)]
                {:ram-used {:used (/ (- MemTotal MemFree Buffers Cached) 1024.0 1024.0)
                            :max (/ MemTotal 1024.0 1024.0)}})))

(defn observe-filesystem [partition-map]
  (for-map [[k ^String n] partition-map
            :let [f (java.io.File. n)
                  i (when (.exists f)
                      (err/?warn "File API not implemented"
                                 (free->used (gb (.getFreeSpace f)) (gb (.getTotalSpace f)))))]
            :when i]
    k i))

(defn observe-jvm []
  (let [rt (Runtime/getRuntime)
        total (.totalMemory rt)
        free (.freeMemory rt)
        max (.maxMemory rt)
        used (- total free)]
    {:heap {:used (gb used) :max (gb max) :cur-size (gb total)}
     :threads {:count (.size (Thread/getAllStackTraces))}}))

(defn observe-system [& [partition-map]]
  (merge-with merge
              (observe-machine)
              (observe-meminfo)
              {:disk (observe-filesystem (or partition-map {:root "/" :mnt "/mnt" :ebs "/ebs" :data "/Volumes/data"}))
               :jvm (observe-jvm)}))

(defn flatten-counter [m]
  (cond (nil? m) nil
        (number? m) m
        (map? m) (for-map [[k v] m
                           :let [vf (flatten-counter v)
                                 k1 (if (number? k) (str k) k)]
                           [k2 v2] (when vf (if (map? vf) vf {nil vf}))]
                   (if k2 (str (name k1) "_" (name k2)) k)
                   v2)
        :else (do (log/warnf "Weird counter %s" m) {"weirddata" 0})))

(defn machine-metrics [observations]
  (flatten-counter
   ((fn [observations]
      (if (map? observations)
        (if-let [m (:max observations)]
          (if (zero? m) 0 (/ (:used observations 0) 1.0 m))
          (map-vals machine-metrics observations))))
    observations)))

(declare register-persistent-metrics!)

(def observer-bundle
  (resource/bundle
   :observation-bucket (fnk [] (bucket/bucket {:type :mem}))
   :drained-atom (fnk [] (atom nil)) ;; sorry
   :the-observer (fnk [observation-bucket drained-atom]
                   (doto (plumbing-observer/make-simple-observer
                          (partial bucket/update observation-bucket)
                          #(get @drained-atom %))
                     (plumbing-observer/report-hook :machine observe-system)))
   :persistent-observer (fnk [log-data-store [:instance service-name]]
                          (persistent-observer log-data-store service-name))
   :simple-aggregations-atom (fnk [] (atom {}))
   :metric-loggers-atom (fnk [] (atom {}))
   :observer-pool (fnk [snapshot-store [:instance service-name] persistent-observer log-data-store
                        simple-aggregations-atom metric-loggers-atom
                        observation-bucket drained-atom the-observer
                        {flush-freq 60}
                        :as args]
                    (let [agg (mongo/aggregate-collection
                               log-data-store "agg-simple" {:agg-keys [:_day :_service :_name]})]
                      (register-persistent-metrics!
                       {:observer-bundle args
                        :agg-fns {:pubsub (comp flatten-counter :counts :pubsub-stats)
                                  :machine (comp machine-metrics :machine)}})
                      (start-flushing!
                       observation-bucket
                       snapshot-store
                       (fn report-fn [results extra-data]
                         (reset! drained-atom results)
                         (let [rep (plumbing-observer/report the-observer extra-data)]
                           (doseq [m (vals @metric-loggers-atom)]
                             (with-ex (logger) m rep))
                           (doseq [[n a] @simple-aggregations-atom]
                             (with-ex (logger)
                               mongo/update agg {:_day (today) :_service service-name :_name n} {} (a rep)))
                           rep))
                       (or flush-freq 60))))))

(defnk observer-resource [[:observer-bundle the-observer]] the-observer)

(defnk register-simple-aggregations! [[:observer-bundle simple-aggregations-atom] agg-fns]
  (s/validate {s/Keyword clojure.lang.AFn} agg-fns)
  (swap! simple-aggregations-atom map/merge-disjoint agg-fns))

(defnk register-persistent-metrics! [[:observer-bundle metric-loggers-atom persistent-observer] agg-fns]
  (s/validate {s/Keyword clojure.lang.AFn} agg-fns)
  (swap! metric-loggers-atom map/merge-disjoint
         (for-map [[k f] agg-fns
                   :when f]
           k
           (comp (metric-logger persistent-observer (name k) {:from-observer true})
                 f))))

(defnk latest-agg-metrics
  "Retrieve rows from the daily aggregated metrics.
   If service-prefix is passed (e.g. for replicated services), sums
   rows across all matching prefixes"
  [log-data-store
   {service nil}
   {service-prefix nil}
   {metric-name nil}
   {metric-key nil}
   {num-days 2}
   {limit 100}
   {fields nil}]
  (assert (= 1 (count (remove nil? [service service-prefix]))))
  (->> (mongo/query
        (mongo/bucket-collection log-data-store +agg-metrics-coll+)
        (assoc-when
         {:query (assoc-when
                  {:service (or service {"$regex" (str "^" service-prefix)})
                   :day {"$gte" (rel-day (- (new-time/to-millis (dec num-days) :day)))}}
                  :name metric-name
                  :key metric-key)
          :limit limit}
         :fields fields))
       (map #(dissoc % :_id))
       (?>> service-prefix
            (#(math/project % [:day :key :name :service] (partial merge-with +) :service service-prefix)))))

(comment
  ;; delete dead services from mongo.
  (let [lds (@#'service.builtin-resources/log-data-store {:env :prod})
        meta-bucket (store.mongo/bucket-collection lds "meta")
        colls (bucket/vals meta-bucket)
        svcs (set (service.nameserver/all-service-names (:nameserver config)))]
    (doseq [c colls]
      (when (and (not (svcs (:service c)))
                 (or #_(not (.startsWith (:service c) "api-prod"))
                     (.endsWith (:full-name c) "pubsub")
                     (.endsWith (:full-name c) "machine")))

        (println "deleting " (:full-name c))
        (mongo/drop-collection lds (:full-name c))
        (bucket/delete meta-bucket (:full-name c))))))




(set! *warn-on-reflection* false)
