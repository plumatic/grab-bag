(ns flop.optimize-function-test-suite-test
  (:require
   [plumbing.core :refer :all]
   [plumbing.test :refer :all]
   [flop.optimize-function-test-suite :refer :all]
   [flop.empirical-gradient :as empirical-gradient]
   [clojure.test :refer :all]))

(deftest function-defs-test
  (doseq [{:keys [f min]} +test-suite+
          [computed empirical] (empirical-gradient/empirical-gradient
                                {:xs min
                                 :num-dims (count min)
                                 :f f
                                 :dx 1e-6})]
    (testing "gradient should be 0 at min"
      (is-approx-= computed 0.0 1e-8))
    (testing "gradient and objective are consistent"
      (is-approx-= computed empirical 1e-3))))
