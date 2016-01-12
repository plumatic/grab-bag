;; Bootstrap things in a way that works around schema multi-class classloader issues.
(require 'schema.core 'crane.config 'plumbing.new-time 'haderp.core)
;; Nuclear option below works if all else fails:
;; (require '[schema.macros])
;; (reset! schema.macros/*compile-fn-validation* false)

(ns example-job.job
  "Example wordcount job copied from clojure-hadoop example wordcount5"
  (:require
   [clojure-hadoop.defjob :as defjob]
   [clojure-hadoop.job :as job]
   [clojure-hadoop.imports :as imports]
   [clojure-hadoop.wrap :as wrap]
   [haderp.core :as haderp])
  (:import
   [java.util StringTokenizer]))

(imports/import-io)
(imports/import-mapreduce)

(defn my-map [key value]
  (map (fn [token] [token 1]) (enumeration-seq (StringTokenizer. value))))

(defn my-reduce [key values-fn]
  [[key (reduce + (values-fn))]])

(defn string-long-writer [^TaskInputOutputContext context ^String key value]
  (.write context (Text. key) (LongWritable. value)))

(defn string-long-reduce-reader [^Text key wvalues]
  [(.toString key)
   (fn [] (map (fn [^LongWritable v] (.get v)) wvalues))])

(defjob/defjob job
  :map my-map
  :map-reader wrap/int-string-map-reader
  :map-writer string-long-writer
  :reduce my-reduce
  :reduce-reader string-long-reduce-reader
  :reduce-writer string-long-writer
  :output-key Text
  :output-value LongWritable
  :input-format :text
  :output-format :text
  :compress-output false
  :replace true)

(def +cluster-config+
  {:instances {:keep-alive? true ;; don't shut down cluster on success/failure, good for debugging
               :core-instances {:num 2 :type "m3.xlarge"}
               ;;:task-instances {:num 2 :type "cc2.8xlarge" :spot-price "1.00"}
               }})

(def +step-config+
  {:input-data (apply concat (repeat 100 ["war and peace" "love and hate" "love and war"]))
   ;; make sure cluster stays alive after failure, for debugging
   :failure-action :cancel-and-wait})


(comment
  (require '[example-job.job :as j] '[aws.emr :as emr] '[haderp.core :as h])

  ;; run in-process
  (h/run-in-jvm! 'example-job.job/job j/+step-config+)
  (plumbing.core/map-vals #(String. % "UTF-8") (:output *1))

  ;; run in local hadoop
  (h/run-locally! 'example-job.job/job j/+step-config+)

  ;; run on EMR.
  (def job-info (h/launch! j/+cluster-config+ 'example-job.job/job j/+step-config+))

  (def try-again (h/run! (:cluster-id job-info) 'example-job.job/job j/+step-config+))

  (emr/job-flow-status @h/emr (:cluster-id job-info))

  (def out (bucket/seq (:output-bucket try-again)))

  ;; (plumbing.parallel/wait-until #(= "WAITING" (doto (emr/job-flow-status @h/emr (:cluster-id job-info)) println)) 300 5)
  )
