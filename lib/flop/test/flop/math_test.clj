(ns flop.math-test
  (:use clojure.test)
  (:require [flop.math :as dm]
            [plumbing.repl :as ru])
  (:import java.util.Random))

(defmacro is-double-approx-= [expr val rel-err abs-err]
  `(do (is (= ~(ru/expression-info expr) {:class Double/TYPE :primitive? true}))
       (let [v# ~expr]
         (when-not (zero? ~val)
           (is (< (/ (Math/abs (- ~val ~expr)) ~val)
                  ~rel-err)))
         (is (< (Math/abs (- ~val ~expr)) ~abs-err)))))

(deftest digamma-test []
  (is-double-approx-= (dm/digamma (double 1)) -0.5772156649015084 1.0e-9 1.0e-9))

(deftest loggamma-test []
  (is-double-approx-= (dm/log-gamma (double 3)) 0.693147180559945309 1.0e-0 1.0e-9))

(deftest sloppy-log-test []
  (doseq [x [1.0e-10 1.0e-5 1.0e-2 0.9999 100]]
    (is-double-approx-= (dm/sloppy-log (double x)) (Math/log x) 1.0e-4 1.0e-4)))

(deftest sloppy-exp-test []
  (doseq [x [-1000 -10 -1.0e-9 1.0e-10 1.0e-5 1.0e-2 0.9999 100]]
    (is-double-approx-= (dm/sloppy-exp (double x)) (Math/exp x) 1.0e-4 1.0e-4)))

(deftest sloppy-exp-negative-test []
  (is (= (dm/sloppy-exp-negative 0) 1.0))
  (doseq [x (range -29.9 -0.1 0.1)]
    (is-double-approx-= (dm/sloppy-exp-negative (double x)) (Math/exp x) 1.0e-3 1.0e-2)))

(deftest telsst-spaese-dot-product
  (is (= 35.0 (dm/sparse-dot-product {:f1 10 :f2 1 :f3 5} {:f1 1 :f3 5})))
  (is (= 0.0 (dm/sparse-dot-product {:f1 10 :f2 1 :f3 5} {:f4 1 :f5 5})))
  (is (= 35.0 (dm/sparse-dot-product {:f1 10 :f2 1 :f3 5} (seq {:f1 1 :f3 5})))))

(deftest deterministic-sample-categorical-test
  (is (= :a (dm/deterministic-sample-categorical 0.5 {1.0 :a})))
  (let [s (fn [p] (dm/deterministic-sample-categorical p [[0.3 :a] [0.2 :b] [0.4 :c] [0.1 :d]]))]
    (is (= :a (s 0.0)))
    (is (= :a (s 0.25)))
    (is (= :b (s 0.31)))
    (is (= :c (s 0.51)))
    (is (= :c (s 0.89)))
    (is (= :d (s 0.91)))
    (is (= :d (s 1.01)))))
