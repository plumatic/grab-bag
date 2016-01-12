(ns plumbing.logging
  (:use plumbing.core)
  (:require
   [clojure.tools.logging :as ctl]))

;; WARNING, PLEASE FIX! Bad alias of clojure.tools.logging: ctl should be log.
;; Removed superflous :import [clojure.lang ExceptionInfo]

;; Small shim in front of clojure.tools.logging with two main changes:
;; 1.  Things directly logged are maps, not strings.
;; 2.  Logging can be forked to several places, with different thresholds.
;; For now, initiailly just uses


;; Log entries all have:
;; :service :level :log :exception
;; Exception must be class when logged, becomes map...

;; Both log and exception have:
;; :file :ns (opt for ex) :line
;; :type :message :data ??
;; type and message should just be part of data?

;; Exceptions have :stack-trace
;; how about class, cause?

;; Stuff should be truncated for mongo ...

;; If you use ex-info, you get data; and type from there overrides.

;;; Exceptions, maps, and messages

(set! *warn-on-reflection* true)

(defn summary-message [m]
  (try
    (let [max-ks 10
          max-vlen 50]
      (let [sb (StringBuffer.)]
        (.append sb "{")
        (doseq [[k v] (take max-ks m)]
          (.append sb (str k))
          (.append sb " ")
          (let [vs (str v)]
            (.append sb ^String (if (> (.length vs) max-vlen)
                                  (str (.substring vs 0 max-vlen) "...")
                                  vs)))
          (.append sb ", "))
        (when (seq m) (.setLength sb (- (.length sb) 2)))
        (when (> (count m) max-ks) (.append sb ", ..."))
        (.append sb "}")
        (.toString sb)))
    (catch Exception e (ctl/warn "Error making log summary message") "meta: message serialization error")))

(defmacro our-ex-info
  "Create an exception carrying data from a map.  m
   may have :type, :message, and :cause, plus any other data you wish."
  [m]
  `(let [m# ~m]
     (clojure.lang.ExceptionInfo.
      (or (:message m#) (summary-message m#))
      (dissoc m# :cause)
      (:cause m#))))

(defmacro throw+ [m] `(throw (our-ex-info ~m)))


;; TODO: location can be overridden somehow, e.g., in graph node XYZ

(defn innermost [^Throwable t & [d]]
  (if-let [c (.getCause t)]
    (innermost c (inc (or d 0)))
    [t 0]))

(defn data-chain
  "Produces a seq of maps, one for each throwable on the cause-chain,
  with each map containing (ex-data t) for that throwable, plus its
  message"
  [^Throwable t]
  (when t
    (cons (merge {:message (.getMessage t)}
                 (ex-data t))
          (lazy-seq (data-chain (.getCause t))))))

(defn class-name [x] (.getName ^Class (class x)))

;; TODO: have location entered clojure, rather than wehre-thrown?

(defn parse-stack-trace-elem [^StackTraceElement e]
  {:file (.getFileName e) :class (.getClassName e) :method (.getMethodName e) :line (long (.getLineNumber e))})

(defn jsonify [form]
  "Convert a Clojure data structure into something jsonable by stringifying non-jsonable things."
  (cond
   (list? form) (apply list (map jsonify form))
   (instance? clojure.lang.IMapEntry form) (let [[k v] form]
                                             [(if (keyword? k) k (str k))
                                              (jsonify v)])
   (or (keyword? form) (string? form)) form
   (seq? form) (doall (map jsonify form))
   (instance? clojure.lang.IRecord form) (jsonify (into {} form))
   (coll? form) (into (empty form) (map jsonify form))
   :else (pr-str form)))

(defn throwable->map [^Throwable t]
  (when t
    (let [[^Throwable inner nesting-depth] (innermost t)
          data (ex-data inner)
          stack-trace (map parse-stack-trace-elem (.getStackTrace inner))]
      (merge (first stack-trace)
             {:nesting-depth nesting-depth
              :outer-class (class-name t)
              :type (or (:type data) (class-name inner))
              :message (if data (:message data) (.getMessage inner))
              :data-chain (jsonify (data-chain t)) ;; data for all the entries on the cause chain
              :data (jsonify data)
              :stack-trace stack-trace}))))

(defmacro with-elaborated-exception [map-form & body]
  `(try ~@body
        (catch Throwable ^Throwable e#
               (let [data# (ex-data e#)]
                 (throw+
                  (merge
                   {:type (class-name e#)
                    :cause e#}
                   (ex-data e#)
                   ~map-form))))))

;;; Forking output -- do it simple for now.

(def +levels+
  {:trace 0
   :debug 1
   :info 2
   :warn 3
   :error 4
   :fatal 5})

(def service "String identifying this service." (atom "UNINITIALIZED"))
(def loggers "A seq of [logging-fn, min-level] pairs]" (atom nil))

(defn init!
  "Init logging, with a set of [logging fn, min-level] pairs.
   Logging fn takes raw maps.
   This ns always logs to clojure.tools.logging as well."
  [service-name custom-loggers]
  (assert (string? service-name))
  (reset! service service-name)
  (reset! loggers (doall (for [[f l] custom-loggers]
                           (do (assert (fn? f))
                               [f (safe-get +levels+ l)]))))
  nil)



(defn default-clojure-logger [m]
  (ctl/log
   (-> m :log :ns)
   (:level m)
   (:exception m)
   (-> m :log :message str)))

(defn log* [level ^Throwable t m location]
  (let [m (merge
           location
           (if (map? m) m (do (assert (string? m)) {:message m})))
        outer {:service @service :level level :log m :exception t :time (millis)}]
    (default-clojure-logger outer)
    (let [processed (update-in outer [:exception] throwable->map)
          level-n   (safe-get +levels+ level)]
      (doseq [[f level] @loggers]
        (when (>= level-n level)
          (f processed))))))

(defn form-location [form]
  {:file *file* :ns (str *ns*) :line (-> form meta :line)})

;; TODO: only when enabled stuff.
(defmacro log
  "Low-level logging fn. m is a map of data, may include :message, :type, or whatever, or a string.
   Allowed keys are :location :file :ns :line :type :message :data.
   Keys about location will be filled in automatically if not provided."
  ([level m] `(log ~level nil ~m))
  ([level ^Throwable t m]
     `(log* ~level ~t ~m ~(form-location &form))))

(defmacro ^:private deflevel [level-key]
  (let [x-sym (gensym "x")]
    `(do (defmacro ~(-> level-key name symbol)
           "Log an optional throwable and map or string."
           {:arglists '([map-or-string] [throwable map-or-string])}
           ([m#] `(log* ~~level-key nil ~m# ~(form-location ~'&form)))
           ([t# m#] `(log* ~~level-key ~t# ~m# ~(form-location ~'&form))))
         (defmacro ~(-> level-key name (str "f") symbol)
           "Log an optional throwable and message with args as to format."
           {:arglists '([& format-args] [throwable & format-args])}
           [x# & args#]
           `(let [~'~x-sym ~x#]
              (if (instance? Throwable ~'~x-sym)
                (log* ~~level-key ~'~x-sym (format ~@args#) ~(form-location ~'&form))
                (log* ~~level-key nil (format ~'~x-sym ~@args#) ~(form-location ~'&form))))))))

(deflevel :trace)
(deflevel :debug)
(deflevel :info)
(deflevel :warn)
(deflevel :error)
(deflevel :fatal)


(set! *warn-on-reflection* false)
