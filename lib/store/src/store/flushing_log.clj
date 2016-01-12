(ns store.flushing-log
  "A flushing log that supports appending a new log-line, and periodically flushes it to a
   backing bucket."
  (:use plumbing.core)
  (:require
   [plumbing.auth :as auth]
   [plumbing.error :as err]
   [plumbing.graph :as graph]
   [store.bucket :as bucket]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(defn fresh-log-cache
  "Construct a new log-cache"
  []
  {:batch-id (format "%s-%s" (millis) (auth/rand-str 4))
   :log-lines []})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(def logging-bundle
  (graph/graph
   :log-cache (fnk [] (atom (fresh-log-cache)))

   :flush-fn (fnk [log-cache bucket {log-flush-size 1000}]
               (fn [& [force?]]
                 (let [[old new] (swap-pair!
                                  log-cache (fnk [log-lines :as log-cache]
                                              (if (>= (count log-lines) (if force? 1 log-flush-size))
                                                (fresh-log-cache)
                                                log-cache)))]
                   (when-not (= new old)
                     (letk [[batch-id log-lines] old]
                       (err/future
                         "flushing log lines"
                         (bucket/put bucket batch-id log-lines)))))))

   :flush-cache (fnk [flush-fn]
                  (reify java.io.Closeable
                    (close [this]
                      (when-let [f (flush-fn true)]
                        @f))))))

(defn log!
  "Append a log-line to the log-cache"
  [logging-bundle log-line]
  (letk [[flush-fn log-cache] logging-bundle]
    (swap! log-cache #(update-in % [:log-lines] conj log-line))
    (flush-fn)))
