(ns domain.experiments-test
  (:use clojure.test plumbing.test plumbing.core)
  (:require
   [domain.experiments :as experiments]))

(deftest stable?-test
  (is (experiments/stable? [[:a 4]] [[:a 4]]))
  (is (experiments/stable? [[:a 4] [:b 6]] [[:a 4] [:b 6]]))
  (is (experiments/stable? [[:a 4] [:b 6]] [[:a 6] [:b 4]]))
  (is (experiments/stable? [[:a 4] [:b 6]] [[:a 4] [:b 4] [:a 2]]))
  (is (experiments/stable? [[:a 4] [:b 6] [:c 10]] [[:a 6] [:b 3] [:c 11]]))
  (is (experiments/stable? [[:a 10]] [[:b 10]]))
  (is (not (experiments/stable? [[:a 5] [:b 5]] [[:b 5] [:a 5]])))
  (is (not (experiments/stable? [[:a 4] [:b 6]] [[:a 3] [:b 4] [:a 3]]))))

(deftest compact-test
  (is (= [[:a 10] [:b 4]]
         (experiments/compact [[:a 2] [:a 3] [:a 5] [:b 2] [:b 2]]))))

(deftest stabilize-test
  (is (= [[:a 6] [:b 4]]
         (experiments/stabilize {:a 6 :b 4} [[:a 4] [:b 6]])))
  (is (= [[:b 4] [:a 6]]
         (experiments/stabilize {:a 6 :b 4} [[:b 6] [:a 4]])))
  (is (= [[:b 10]]
         (experiments/stabilize {:b 10} [[:a 1] [:b 3] [:c 6]])))
  (is (= [[:a 1] [:c 2] [:b 2] [:c 1]]
         (experiments/stabilize {:a 1 :b 2 :c 3} [[:a 3] [:b 2] [:c 1]]))))

(deftest sample-test
  (testing "Rough test for proportions and independence of multiple tests."
    (let [t1 (seq {true 2 false 2})
          t2 (seq {:a 1 :b 2 :c 1})
          tests [[:t1 t1] [:t2 t2]]
          n 10000
          outcomes (frequencies
                    (for [i (range n)]
                      (for [[tn td] tests]
                        (experiments/sample td (str i tn)))))]
      (is-= (* (count t1) (count t2)) (count outcomes))
      (doseq [[v1 f1] t1 [v2 f2] t2]
        (let [expected (/ (* n f1 f2) 16.0)]
          (is (< (* expected 0.8) (outcomes [v1 v2]) (* expected 1.2)))))))

  (testing "stability of sampling"
    (is-= 240
          (experiments/sample (for [i (range 1000)] [i 1]) "1lkj1l223kla"))))

(deftest experiments-test
  (let [e (-> (experiments/experiment "test-experiment" "a description" 100 (array-map :a 4 :b 6))
              (experiments/modify 200 {:a 6 :b 4})
              (experiments/modify 300 {:c 10})
              (experiments/complete 400 :d))
        d1 [[:a 4] [:b 6]]
        d2 [[:a 6] [:b 4]]
        d3 [[:c 10]]
        d4 [[:d 10]]]
    (testing "Final experiment"
      (is-= {:name "test-experiment"
             :description "a description"
             :distributions [{:start-date 100 :distribution d1}
                             {:start-date 200 :distribution d2}
                             {:start-date 300 :distribution d3}
                             {:start-date 400 :distribution d4}]}
            e))
    (testing "getting distributions"
      (is (nil? (experiments/distribution-at e 10)))
      (is (= d1 (experiments/distribution-at e 100)))
      (is (= d1 (experiments/distribution-at e 199)))
      (is (= d4 (experiments/distribution-at e 10000)))
      (is (= d4 (experiments/latest-distribution e))))
    (testing "sampling"
      (is (nil? (experiments/sample-at e "ASDF" 50)))
      (is (= :c (experiments/sample-at e "ASDF" 350)))
      (is (= :d (experiments/sample-latest e "ASDF"))))
    (testing "stability of sampling"
      (is (= [:b :a :a :b :b :b :a :b :b :b]
             (for [i (range 10)] (experiments/sample-at e (str i) 150)))))))


(use-fixtures :once validate-schemas)
