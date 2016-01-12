(ns classify.algorithms.regression-test
  (:use clojure.test plumbing.core classify.algorithms.regression plumbing.test)
  (:require
   [flop.map :as map]
   [flop.empirical-gradient :as empirical-gradient])
  (:import [flop LongDoubleFeatureVector]))

(def sample-datums
  [ [(map/map->fv {0 1.0}) 3 1.0]
    [(map/map->fv {1 1.0 2 1.0}) 3 1.0]
    [(map/map->fv {0 1.0 1 1.0 2 1.0}) 6 1.0]])


(deftest linear-objective-test
  (let [project (juxt first (comp seq second))]

    (testing "Should have some error, all positive gradients"
      (is (= [(sum #(double (* 0.5 % %)) (map second sample-datums))
              [-9.0 -9.0 -9.0]]
             (project
              (linear-objective
               3
               sample-datums
               (double-array [0.0 0.0 0.0]))))))

    (testing "Should have zero error"
      (is (= [0.0 [0.0 0.0 0.0]]
             (project
              (linear-objective
               3
               sample-datums
               (double-array [3.0 1.5 1.5]))))))))

(deftest objective-gradient-consistency-test
  (let [num-dims 3
        f (partial linear-objective num-dims sample-datums)
        gradients (empirical-gradient/empirical-gradient {:num-dims 3 :f f})]
    (doseq [[grad emp-grad] gradients]
      (is-approx-= grad emp-grad 1e-2))))

(defn generate-example [^java.util.Random rand
                        ^doubles mean-vec]
  (let [fv (map/map->fv (for-map [idx (range (count mean-vec))]
                          idx (.nextDouble rand)))]
    ;; Sample is dot-product plus some residual noise
    [fv (+ (.dotProduct fv mean-vec)
           (* 0.1 (.nextGaussian rand)))]))

(defn learned-weights [data]
  (-> data (learn-linear {}) .ldhm map/trove->map))

(defn recover-params-test [^java.util.Random rand ^doubles mean-vec ^long num-examples]
  (let [rand (java.util.Random. 0)
        mean-vec (double-array [1.0 2.0 3.0])
        data (map (fn [_] (generate-example rand mean-vec))
                  (range num-examples))]
    ;; Test learned params are close to generating
    (is-approx-=
     (into {} (map-indexed vector mean-vec))
     (learned-weights data)
     0.01)))

(deftest recovery-test
  (let [rand (java.util.Random. 0)]
    (recover-params-test
     rand
     (double-array [1.0 2.0 3.0])
     1e3)))

(deftest straight-line-test
  (is-approx-=
   (learned-weights (for [i (range 100)]
                      [(flop.map/map->fv {0 (double i) 1 1.0}) (+ (* 2.0 i) 3.0) 1.0]))
   {0 2.0 1 3.0}
   1.0e-6))
