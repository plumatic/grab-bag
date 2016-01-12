(ns flop.optimize-test
  (:use clojure.test plumbing.test flop.optimize)
  (:require
   [flop.optimize-function-test-suite :as optimize-function-test-suite]))

(deftest remember-last-test
  (let [c (atom 0)
        f (fn [x] (swap! c inc) (+ x 5))
        r-l (remember-last f)]
    (is (= (r-l 2) 7))
    (is (= @c 1))
    (r-l 2)
    (r-l 2)
    (r-l 2)
    (is (= @c 1))
    (is (= (r-l 3) 8))
    (is (= @c 2))
    (r-l 2)
    (is (= @c 3)))
  (let [c (atom 0)
        f (fn [x] (swap! c inc) x)
        within4 (fn [x y] (< (Math/abs (double (- x y))) (double 1e-4)))
        within10 (fn [x y] (< (Math/abs (double (- x y))) (double 1e-10)))
        r-l (remember-last f (fn [x] x) within4)]
    (is (within10 (r-l 1.0) 1.0))
    (is (= @c 1))
    (is (within10 (r-l 1.0) 1.0))
    (is (= @c 1))
    (is (within10 (r-l 1.000001) 1.0))
    (is (= @c 1))
    (is (within10 (r-l 1.2) 1.2))
    (is (= @c 2))
    (is (within10 (r-l 1.0) 1.0))
    (is (= @c 3))))

(deftest affine-fn-test
  (let [f1 (affine-fn (double-array [1 2 3])  (double-array [5 6 7]))]
    (is (= (seq (f1 5)) [26.0 32.0 38.0]))))

(deftest step-fn-test
                                        ; f(x,y) = x^2 + 2*y
  (let [f (fn [[x y]] (+ (* x x) (* 2 y)))
        s-f (step-fn f (double-array [1 1]) (double-array [1 0]))]
    (is (= (s-f 1) 6.0))
    (is (= (s-f 0.5) 4.25))))

(deftest backtracking-line-search-test
                                        ; f(x) = x^2 - 1
  (let [f (fn [arr] (let [x (first arr)] [(- (* x x) 1) (double-array [(* 2 x)])]))
        alpha (backtracking-line-search f (double-array [0]) (double-array [1]) {})
        step-fn (step-fn f (double-array [0]) (double-array [1]))]
    (is-approx-= (first (step-fn alpha)) -1 1.0e-4)))

(deftest ^:flaky lbfgs-test
  ;; flaky: occasionally gets non-positive curvature, regardless of initializer
  (doseq [{:keys [f min]} optimize-function-test-suite/+test-suite+
          :let [init (double-array (repeatedly (count min) #(rand)))]]
    (is-approx-= min (lbfgs-optimize f init) 1e-7)))
