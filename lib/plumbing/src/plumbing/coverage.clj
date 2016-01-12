(ns plumbing.coverage
  "Simple utility for measuring test coverage by instrumenting
   functions to count calls."
  (:require
   [clojure.test :as test]
   [clojure.tools.namespace :as namespace]
   [plumbing.core :as plumbing]))

(defn eligible-fn? [x]
  (and (fn? x)
       (not-any? #(instance? % x)
                 [clojure.lang.IFn$LLL
                  clojure.lang.IFn$L
                  clojure.lang.IFn$OOD
                  clojure.lang.IFn$OD
                  clojure.lang.IFn$LD
                  clojure.lang.IFn$DD
                  clojure.lang.IFn$DDDD
                  clojure.lang.IFn$OODO
                  clojure.lang.IFn$OLO
                  clojure.lang.IFn$LLO
                  clojure.lang.IFn$LLOO
                  ])))


(defn fn-vars-by-ns [^String ns-prefix]
  (plumbing/for-map [n (all-ns)
                     :when (or (not ns-prefix) (.startsWith (name (ns-name n)) ns-prefix))]
    (ns-name n)
    (plumbing/for-map [[s v] (ns-interns n)
                       :when (eligible-fn? @v)]
      s v)))

(defn call-report [fn-vars-by-ns call-counts]
  {:call-counts call-counts
   :raw-uncalled fn-vars-by-ns
   :uncalled (plumbing/for-map [[k fs] (merge-with (fn [fns calls] (remove calls fns))
                                                   (plumbing/map-vals keys fn-vars-by-ns)
                                                   call-counts)
                                :when (not (.endsWith (name k) "test"))]
               k fs)})

(defmacro with-coverage-report [ns-prefix & body]
  `(let [fv# (fn-vars-by-ns ~ns-prefix)
         cc# (atom {})]
     (with-redefs-fn (plumbing/for-map [[n# n-map#] fv#
                                        [s# v#] n-map#]
                       v#
                       (let [f# @v#]
                         (with-meta
                           (fn [& args#]
                             (swap! cc# update-in [n# s#] (fnil inc 0))
                             (apply f# args#))
                           (meta f#))))
       (fn [] ~@body))

     (call-report fv# @cc#)))

(defn load-all-files [^String ns-prefix]
  (apply require (filter #(.startsWith (name %) ns-prefix) (namespace/find-namespaces-on-classpath))))

(defn test-coverage-report [ns-prefix]
  (load-all-files ns-prefix)
  (with-coverage-report ns-prefix
    (test/run-all-tests)))
