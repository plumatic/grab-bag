(ns plumbing.timing
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [plumbing.logging :as log]))

(set! *warn-on-reflection* true)

(defmacro with-time
  "Evaluates expr, binding its value to v and the time taken (in ms) to tv"
  [tv [v expr] & body]
  `(let [start# (System/nanoTime)
         ~v     ~expr
         ~tv    (/ (double (- (System/nanoTime) start#)) 1000000.0)]
     ~@body))

(defmacro get-time [expr] `(with-time t# [_# ~expr] t#))

(defmacro get-time-pair [expr] `(with-time t# [v# ~expr] [v# t#]))

(defn log-time [tag f & args]
  (with-time t [v (apply f args)]
    (log/infof "%s took %.3f secs" tag (/ t 1000.0))
    v))



;;; Microbenchmarking.

(defn benchmark-fn-form* [stop? consume form]
  `(fn bench-it []
     (let [stop?# ~stop?
           consume# ~consume
           loop-start# (System/nanoTime)]
       (loop [it# 0 time# 0 consumed# 0.0]
         (if (stop?# it# time#)
           {:iterations it#
            :ns time#
            :wall-ns (- (System/nanoTime) loop-start#)
            :consumed consumed#}
           (let [start# (System/nanoTime)
                 v# ~form
                 stop# (System/nanoTime)]
             (recur (unchecked-inc it#)
                    (+ time# (- stop# start#))
                    (+ consumed# (consume# v#)))))))))

(defn benchmark-fn-form [opts form]
  (let [consume-output (:consume-output opts)]
    (benchmark-fn-form*
     (if-let [its (:iterations opts)]
       (let [its (long its)]
         `(fn [i# t#] (>= (long i#) ~its)))
       (let [mt (* 1000000 (long (:min-ms opts)))]
         `(fn [i# t#] (>= (long t#) ~mt))))
     consume-output
     form)))

(defn benchmark-correcter-form [it-fraction]
  (let [its (gensym "its")]
    `(let [~its (atom nil)
           empty-bench# ~(benchmark-fn-form*
                          `(let [oi# (long @~its)]
                             (fn [i# t#] (>= (long i#) oi#)))
                          `identity 1.0)]
       (fn [results#]
         (let [its# (quot (:iterations results#) ~it-fraction)]
           (reset! ~its its#)
           (let [empty-results# (empty-bench#)]
             (assert (= ~its))
             (assoc results#
               :overhead-iterations its#
               :overhead-ns (:ns empty-results#))))))))

(defn corrected-benchmark-fn-form [opts form]
  `(let [bf# ~(benchmark-fn-form opts form)
         bc# ~(benchmark-correcter-form (:it-fraction opts))]
     (fn [] (bc# (bf#)))))

(defmacro simple-bench [iterations form]
  `(~(corrected-benchmark-fn-form {:iterations iterations} form)))

(defn crunch-result [result]
  (letk [[overhead-ns overhead-iterations wall-ns ns iterations] result]
    (let [measured-ms (/ ns 1.0e6 iterations)
          overhead-ms (/ overhead-ns 1.0e6 overhead-iterations)]
      {:iterations iterations
       :avg-ms (- measured-ms overhead-ms)
       :raw-ms measured-ms
       :percent-overhead (* 100 (/ overhead-ms measured-ms))})))

(defn mean [xs]
  (/ (sum xs) 1.0 (count xs)))

(defn sq [x] (* x x))
(defn sd [xs]
  (let [m (mean xs)]
    (Math/sqrt (- (/ (sum sq xs) 1.0 (count xs)) (* m m)))))

(defn stats [xs]
  (let [m (mean xs)]
    (format "%f (+- %2.1f%%)" m (if (zero? m) 0.0 (* 100.0 (/ (sd xs) (mean xs)))))))

(defn raw-table-data [results]
  (for [[form data] results]
    (let [crunched (map crunch-result data)]
      {:form form
       :ms-mean (mean (map :avg-ms crunched))
       :ms-sd (sd (map :avg-ms crunched))
       :ms (stats (map :avg-ms crunched))
       :iterations (stats (map :iterations crunched))
       :raw-ms (stats (map :raw-ms crunched))
       :percent-overhead (stats (map :percent-overhead crunched))})))

(defn with-slowness [table-data]
  (let [fastest (apply min-key :ms-mean table-data)]
    (for [t table-data]
      (assoc t
        :slowness
        (if (= (:form fastest) (:form t))
          1
          (let [fm (:ms-mean fastest)
                fs (:ms-sd fastest)
                tm (:ms-mean t)
                ts (:ms-sd t)
                slowness (/ tm fm)]
            (format "%f (+- %2.1f%%)"
                    slowness
                    (* 100.0
                       (Math/sqrt (+ (sq (/ fs fm)) (sq (/ ts tm))))))))))))

(defn report-benchmark-results [results]
  (pprint/print-table
   [:form :slowness :ms :iterations :percent-overhead :raw-ms]
   (with-slowness (raw-table-data results))))

(defn collect-results [reps fns]
  (let [results (java.util.IdentityHashMap.)]
    (dotimes [_ reps]
      (doseq [[k f] (shuffle fns)]
        (.put results k (conj (or (.get results k) []) (f)))))
    (for [[k] fns]
      [k (.get results k)])))

(defn microbenchmark-results-form [opts & forms]
  `(let [fns# ~(vec (for [[i form] (indexed forms)] [`'~form (corrected-benchmark-fn-form opts form)]))]
     (collect-results ~(:warmup-reps opts) fns#)
     (collect-results ~(:reps opts) fns#)))

(defn generic-consume [x] (count (str x)))

(def +default-opts+
  {:consume-output #_`identity `generic-consume
   :warmup-reps 2
   :reps 5
   :min-ms 200
   :it-fraction 1})

(defmacro microbenchmark
  "Microbenchmark forms.
   Opts has
     :consume-output fn, identity by default,
     :warmup-reps, 2 by default
     :reps, 5 by default
     :iterations, automatically set per expr from min-ms by default,
     :min-ms, 200 by default
     :it-fraction, 1 by default (speeds up but probably not worth using.)
   Each iteration is done in a random order."
  [opts & forms]
  (if (map? opts)
    `(report-benchmark-results
      ~(apply microbenchmark-results-form (merge +default-opts+ opts) forms))
    `(microbenchmark {} ~opts ~@forms)))

(set! *warn-on-reflection* false)