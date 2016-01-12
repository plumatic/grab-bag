(ns plumbing.math-test
  (:use clojure.test plumbing.math plumbing.test))

(deftest entropy-test
  (let [dist (fn [xs]
               (map #(double (/ % (count xs)))
                    (vals (frequencies xs))))]

    (testing "dist"
      (is-=-by sort [0.5 0.5] (dist [1 1 0 0]))
      (is-=-by sort [0.25 0.75] (dist [1 1 1 0])))

    (testing "entropy"
      (is-= 0.0 (entropy (dist [1 1 1 1])))
      (is-= 1.0 (entropy (dist [1 0])))
      (is-approx-= 0.72193 (entropy (dist [1 0 0 0 0]))))))

(deftest mutual-information-test
  (is-= 0.0 (mutual-information
             {[0 0] 4
              [0 1] 4
              [1 0] 4
              [1 1] 4}))
  (is-= 1.0 (mutual-information
             {[0 0] 4
              [0 1] 0
              [1 0] 0
              [1 1] 4}))
  (is-approx-= 0.0001105
               (mutual-information
                {["poultry" "export"] 49
                 ["poultry" "!export"] 141
                 ["!poultry" "export"] 27652
                 ["!poultry" "!export"] 774106})
               1.0e-6))

(deftest mean-test
  (is (= 1 (mean [1])))
  (is (= 1 (mean [0 2])))
  (is (= 1 (mean [0 0 0 4]))))

(deftest median-test
  (is (= 2 (median [9 2 0 1 7])))
  (is (= 2 (median [2]))))

(deftest mode-test
  (is (= 9 (mode [1 8 7 9 2 3 9 12]))))

(deftest sum-od-test
  (is (= 9.0 (sum-od (fn ^double [x] (inc (double x))) [1 2 3]))))

(deftest order-of-magnitude-test
  (is (= 0 (order-of-magnitude -10 0.2 5 4)))
  (is (= 0 (order-of-magnitude 0.1 0.2 5 4)))
  (is (= 1 (order-of-magnitude 0.3 0.2 5 4)))
  (is (= 1 (order-of-magnitude 0.9 0.2 5 4)))
  (is (= 3 (order-of-magnitude 24.0 0.2 5 4)))
  (is (= 4 (order-of-magnitude 124.0 0.2 5 4)))
  (is (= 4 (order-of-magnitude 1240.0 0.2 5 4))))

(deftest normalize-vals-test
  (is-= nil (normalize-vals {}))
  (is-= nil (normalize-vals {:a 1 :b -1}))
  (is-= nil (normalize-vals {:a 0 :b 0}))
  (is-= {:a 1} (normalize-vals {:a 42}))
  (is-= {:a 10/45 :b 20/45 :c 15/45} (normalize-vals {:a 10 :b 20 :c 15})))

(deftest histogram-test
  (is-= nil (histogram [] [10 20 30] 3))
  (is-= {"<=010" 1} (histogram [5] [10 20 30] 3))
  (is-= {"<=010" 2/6 "<=030" 1/6 ">30" 3/6} (histogram [5 100 22 90 7 110] [10 20 30] 3)))

(deftest round-test
  (is-= 1.23 (round 1.23456 2))
  (is-= -1.23 (round -1.23456 2))
  (is-= 1.3 (round 1.256 1))
  (is-= 12000.0 (round 12345 -3)))

(deftest fast-discretizer-test
  (let [d (fast-discretizer [3 11 22])]
    (is-= 0 (d 1))
    (is-= 1 (d 3))
    (is-= 1 (d 3.5))
    (is-= 1 (d 10))
    (is-= 2 (d 20))
    (is-= 3 (d 2000000)))
  (is-= 2 ((fast-discretizer [1 2.2 3.3]) 2.3))
  (let [d (fast-discretizer [3 11 22] true)]
    (is-= 0 (d 1))
    (is-= 0 (d 3))
    (is-= 1 (d 3.1))
    (is-= 1 (d 11))
    (is-= 2 (d 11.1))))

(deftest quantiles-test
  (is-= [0.0 1.0 5.0 10.0] (quantiles (range 11) [0 0.15 0.5 1.0])))

(deftest compact-str-test
  (is (= "1" (compact-str 1)))
  (is (= "1" (compact-str 1.0000)))
  (is (= "2.1" (compact-str 2.1)))
  (is (= "0.0000000123" (compact-str 0.0000000123)))
  (is (= "-2.2" (compact-str -2.2))))

(deftest project-test
  (is-=-by set
           #{{:f1 :a :f2 :bob :x 1 :y 2}
             {:f1 :b :f2 :bob :x 4 :y 4}}
           (project
            [{:f1 :a :f2 :annie :x 1 :y 2}
             {:f1 :b :f2 :annie :x 3 :y 4}
             {:f1 :b :f2 :jeff :x 1}]
            [:f1 :f2]
            (partial merge-with +)
            :f2 :bob)))
