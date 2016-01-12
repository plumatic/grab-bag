(ns plumbing.error
  (:refer-clojure :exclude [future])
  (:use plumbing.core)
  (:require
   [clojure.set :as set]
   [clojure.string :as str]
   [plumbing.logging :as log])
  (:import
   [org.apache.log4j ConsoleAppender Logger PatternLayout]))

(set! *warn-on-reflection* true)

;; NOTE:
;; one confusing thing to note about this ns, is that some functions return new functions
;; and other functions execute the given function.
;; for example, err/with-retries-fn returns a new function that, when called, does the retries if needed
;; while, err/with-ex actually executes the given function.
;;
;; also, be sure to check out all the macros starting with ?, as they will save you effort writing log statements


(defn from-var
  "convert fn variable to [ns-name fn-name] string pair"
  [fn-var]
  (let [m (meta fn-var)]
    [(str (:ns m)) (str (:name m))]))

(defn gen-id [f]
  (let [name (from-var f)]
    (if (every? empty? name)
      "lambda"
      (str/join "/" name))))

(defn assert-keys [ks m]
  (let [missing (set/difference (set ks) (set (keys m)))]
    (when-not (empty? missing)
      (throw (Exception. (format "The Map argument %s is missing keys: %s" (pr-str m) (pr-str missing)))))))

(defn silent [f & args]
  (try
    (apply f args)
    (catch Throwable _ nil)))

(defn cause [^Throwable e]
  (if-let [c (.getCause e)]
    (recur c)
    e))

(defn ex-name [e]
  (.getName (class e)))

(defn apache-level [level]
  (case level
    :all org.apache.log4j.Level/ALL
    :debug org.apache.log4j.Level/DEBUG
    :error org.apache.log4j.Level/ERROR
    :fatal org.apache.log4j.Level/FATAL
    :info org.apache.log4j.Level/INFO
    :off org.apache.log4j.Level/OFF
    :trace org.apache.log4j.Level/TRACE
    :trace-int org.apache.log4j.Level/TRACE_INT
    :warn org.apache.log4j.Level/WARN))

(defn set-log-level!
  ([level]
     (set-log-level! [(org.apache.log4j.Logger/getRootLogger)] level))
  ([loggers level]
     (let [loggers (map (fn [l] (if (string? l)
                                  (org.apache.log4j.Logger/getLogger ^String l)
                                  l))
                        loggers)]
       (doseq [l loggers]
         (.setLevel ^Logger l (apache-level level))))))

(defn init-logger! [& [level]]
  (doto (Logger/getRootLogger)
    (.removeAllAppenders)
    (.addAppender (ConsoleAppender. (PatternLayout. "%-5p %d [%c]: %m%n")
                                    ConsoleAppender/SYSTEM_OUT)))
  (when level (set-log-level! level)))

(defn set-log-level-regex! [regex level]
  (doseq [^org.apache.log4j.Logger l (enumeration-seq (org.apache.log4j.LogManager/getCurrentLoggers))]
    (when (re-matches regex (.getName l))
      (.setLevel l (apache-level level)))))

(defn stfu-apache []
  (set-log-level-regex! #".*apache.*" :fatal))

(defn clean-stack [stack-trace]
  (let [bad-pieces ["swank" "user$eval" "clojure." "java."]]
    (remove
     (fn [^String trace-elem]
       (some (fn [^String bad] (.contains trace-elem bad)) bad-pieces))
     stack-trace)))

(defn truncate [max-length s]
  (if (<= (.length ^String s) max-length)
    s
    (.substring ^String s 0 max-length)))

(defn truncate-walk [max-length xs]
  (cond
   (string? xs) (truncate max-length xs)
   (map? xs) (map-vals (partial truncate-walk max-length) xs)
   (coll? xs) (map (partial truncate-walk max-length) xs)
   :else xs))

(defn print-all [max-length e id args]
  {:ex (str e)
   :fn id
   :args (map pr-str (truncate-walk max-length args))
   :stack (map str (.getStackTrace ^Throwable e))})

(defn atom-logger [& {:keys [report]
                      :or
                      {report (fn [e id args]
                                (str e))}}]
  (let [a (atom "")
        l (fn [e id args]
            (swap! a
                   (fn [x]
                     (report e id args))))]
    [a l]))

(defn logger [&
              {:keys [level report max-length clean-stack]
               :or
               {report print-all
                clean-stack clean-stack
                level :error
                max-length 100}}]
  (fn [e id args]
    (let [m (-> (report max-length (cause e) id args)
                (update-in [:stack] clean-stack))]
      (log/log level (pr-str m)))))

(defn with-ex [h f & args]
  "takes a handler h and a function f."
  (let [{:keys [f id]}
        (if (map? f) f
            {:f f})
        id (or id (gen-id f))]
    (try
      (apply f args)
      (catch java.lang.Throwable e
        (h e id args)))))

(defmacro ?log [log-level m & body]
  `(try ~@body (catch Throwable t# (log/log* ~log-level t# ~m ~(log/form-location &form)) nil)))

(defmacro ?trace [m & body] `(?log :trace ~m ~@body))
(defmacro ?debug [m & body] `(?log :debug ~m ~@body))
(defmacro ?info [m & body] `(?log :info ~m ~@body))
(defmacro ?warn [m & body] `(?log :warn ~m ~@body))
(defmacro ?error [m & body] `(?log :error ~m ~@body))
(defmacro ?fatal [m & body] `(?log :fatal ~m ~@body))
(defmacro ?silent [& body] `(try ~@body (catch Throwable t# nil)))


(defmacro future [m & body]
  `(clojure.core/future (?error ~m ~@body)))

(defmacro -?>
  "first position threaded operator which short-circuits
   on nil or on exception and returns nil in that case"
  ([x] x)
  ([x form] (if (seq? form)
              (with-meta
                `(try
                   (~(first form) ~x ~@(next form))
                   (catch Throwable e# nil))
                (meta form))
              `(try
                 (~form ~x)
                 (catch Throwable e# nil))))
  ([x form & more] `(when-let [f# (-?> ~x ~form)] (-?> f# ~@more))))

(defn with-retries-fn
  "try up to max-tries for each invocation, different from with-give-up since
   that function stops trying for all inputs after it fails enough. This
   just gives up for a given argument afte max-tries"
  ([max-tries f] (with-retries-fn max-tries 0 (constantly nil) f))
  ([max-tries on-fail f] (with-retries-fn max-tries 0 on-fail f))
  ([max-tries delay-ms on-fail f]
     (fn [& args]
       (let [last-ex (atom nil)]
         (loop [num-errs 0]
           (if (= num-errs max-tries) (on-fail @last-ex)
               (let [res (try (apply f args)
                              (catch Exception ex
                                (reset! last-ex ex)
                                (when (> delay-ms 0)
                                  (Thread/sleep delay-ms))
                                ::exception))]
                 (if (not= res ::exception)
                   res
                   (recur (inc num-errs))))))))))

(defmacro with-retries
  "like with-retries-fn, but the last arg is the body to execute, and directly does the
   calling rather than returning a function to be called."
  [& args]
  `((with-retries-fn ~@(butlast args) (fn [] ~(last args)))))

(defmacro assert-is
  "Like assert, but prints some more info about the offending form (may multiple eval on error).
   Starts a debug REPL if swank is loaded."
  ([form] `(assert-is ~form ""))
  ([form format-str & args]
     `(when-not ~form
        (throw (Exception. (str (format ~format-str ~@args)
                                ": Got " '~form " as " (cons '~(first form) (list ~@(next form)))))))))
