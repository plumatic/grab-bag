(ns classify.probabilistic-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [flop.map :as map]
   [classify.features :as features]
   [classify.learn :as learn]
   [classify.probabilistic :as probabilistic]))

(deftest conditional-entropy-evaluator-test
  (let [ce (fn [expected actual]
             (learn/evaluate
              probabilistic/conditional-entropy-evaluator
              (probabilistic/->ConstantModel expected)
              [:ignore actual 1.0]))]
    (is-= Double/POSITIVE_INFINITY (ce 0.0 0.5))
    (is-= 0.0 (ce 0.0 0.0))
    (is-= 1.0 (ce 0.5 0.2))
    (is-= 1.0 (ce 0.5 0.5))
    (is (< (ce 0.1 0.1) (ce 0.09 0.1)))
    (is (< (ce 0.1 0.1) (ce 0.11 0.1)))))

(def test-data
  (let [feats (map/map->fv {0 1.0})]
    [[feats 0.1 1.0]
     [feats 0.2 2.0]
     [feats 0.7 1.0]]))

(deftest constant-trainer-test
  (is-approx-= 0.3
               (:mean (probabilistic/+constant-trainer+ test-data))
               1.0e-10))

(deftest soft-maxent-calibration-test
  (is-approx-= 0.3
               (learn/label
                ((probabilistic/soft-maxent-trainer
                  {:sigma-sq 10000000.0
                   :feature-set (features/feature-set
                                 [(features/feature-fn
                                   :bla
                                   features/default-index
                                   (constantly {}))])})
                 test-data)
                (map/map->fv {0 1.0}))
               1.0e-5))

;; TODO: more tests for soft maxent
