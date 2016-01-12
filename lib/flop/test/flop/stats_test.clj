(ns flop.stats-test
  (:use clojure.test flop.stats plumbing.core plumbing.test)
  (:require [flop.math :as math]))

(deftest add-uni-obs-test
  (let [uni-stats +empty-uni-stats+
        uni-stats1 (add-obs uni-stats 1.5)
        uni-stats2 (add-obs uni-stats1 2.5)]
    (is (math/within 1e-10 (.sum-xs uni-stats1) 1.5))
    (is (math/within 1e-10 (.sum-sq-xs uni-stats1) 2.25))
    (is (math/within 1e-10 (.min-x uni-stats1) 1.5))
    (is (math/within 1e-10 (.max-x uni-stats1) 1.5))
    (is (= (.num-obs uni-stats1) 1.0))

    (is (math/within 1e-10 (.sum-xs uni-stats2) 4.0))
    (is (math/within 1e-10 (.sum-sq-xs uni-stats2) 8.5))
    (is (math/within 1e-10 (.min-x uni-stats2) 1.5))
    (is (math/within 1e-10 (.max-x uni-stats2) 2.5))
    (is (= (.num-obs uni-stats2) 2.0))))

(deftest merge-stats-test
  (let [uni-stats +empty-uni-stats+
        uni-stats1 (add-obs uni-stats 1.5)
        uni-stats2 (add-obs uni-stats1 2.5)
        uni-stats3 (merge-stats uni-stats1 uni-stats2)]
    (is (math/within 1e-10 (.sum-xs uni-stats3) 5.5))
    (is (math/within 1e-10 (.sum-sq-xs uni-stats3) 10.75))
    (is (math/within 1e-10 (.min-x uni-stats3) 1.5))
    (is (math/within 1e-10 (.max-x uni-stats3) 2.5))
    (is (= (.num-obs uni-stats3) 3.0))))

(deftest uni-stats-test
  (is (= (map->UnivariateStats {:sum-xs 0.0 :sum-sq-xs 0.0 :min-x 0.0 :max-x 0.0 :num-obs 3.0})
         (uni-stats [0 0 0])))
  (is (= (map->UnivariateStats {:sum-xs 2.0 :sum-sq-xs 4.0 :min-x 2.0 :max-x 2.0 :num-obs 1.0})
         (uni-stats [2])))
  (is (= (map->UnivariateStats {:sum-xs 2.0 :sum-sq-xs 2.0 :min-x 0.0 :max-x 1.0 :num-obs 4.0})
         (uni-stats [0 0 1 1]))))

(deftest two-sided-t-95-test
  (is (thrown? Exception (two-sided-t-95 1)))
  (is (= (two-sided-t-95 2) 12.71))
  (is (= (two-sided-t-95 45) 2.009))
  (is (= (two-sided-t-95 10000) 1.96)))

(deftest uni-sample-mean-var-test
  (let [uni-stats (reduce add-obs +empty-uni-stats+ [1.0 2.0 3.0 4.0 5.0])
        [mean var] (uni-sample-mean-var uni-stats)]
    (is (math/within 1e-10 mean 3.0))
    (is (math/within 1e-10 var 2.5))))

(deftest uni-conf-interval-test
  (let [uni-stats (reduce add-obs +empty-uni-stats+ [1.0 2.0 3.0 4.0 5.0])
        [lower upper] (uni-conf-interval uni-stats)
        z (/ (* 2.776 (Math/sqrt 2.5)) (Math/sqrt (.num-obs uni-stats)))]
    (is (math/within 1e-10 lower (- 3.0 z)))
    (is (math/within 1e-10 upper (+ 3.0 z)))))

(deftest add-xy-obs-test
  (let [a +empty-bi-stats+
        b (add-xy-obs a [2 5])
        c (add-xy-obs b [10 100])]
    (is-=
     {:sum-xs 12.0
      :sum-sq-xs 104.0
      :sum-ys 105.0
      :sum-sq-ys (double (+ (* 100 100) (* 5 5)))
      :sum-xys (double (+ (* 2 5) (* 10 100)))
      :num-obs 2}
     c)))

(deftest merge-bi-stats-test
  (let [a (bi-stats [[2 5]])
        b (bi-stats [[10 100]])]
    (is-=
     {:sum-xs 12.0
      :sum-sq-xs 104.0
      :sum-ys 105.0
      :sum-sq-ys (double (+ (* 100 100) (* 5 5)))
      :sum-xys (double (+ (* 2 5) (* 10 100)))
      :num-obs 2}
     (merge-bi-stats a b))))

(deftest bi-stats-test
  (is-=
   {:sum-xs 12.0
    :sum-sq-xs 104.0
    :sum-ys 105.0
    :sum-sq-ys (double (+ (* 100 100) (* 5 5)))
    :sum-xys (double (+ (* 2 5) (* 10 100)))
    :num-obs 2}
   (bi-stats [[2 5] [10 100]])))

(deftest bi-report-test
  ;; test data and solution vals from http://en.wikipedia.org/wiki/Simple_linear_regression
  ;; r-xy was computed via google sheets correl function
  (let [xys [[1.47  52.21]
             [1.5 53.12]
             [1.52  54.48]
             [1.55  55.84]
             [1.57  57.2]
             [1.6 58.57]
             [1.63  59.93]
             [1.65  61.29]
             [1.68  63.11]
             [1.7 64.47]
             [1.73  66.28]
             [1.75  68.1]
             [1.78  69.92]
             [1.8 72.19]
             [1.83  74.46]]
        n (count xys)
        mean (fn [f xs] (/ (sum f xs) (count xs)))
        square (fn [z] (* z z))

        mean-x (mean first xys)
        mean-y (mean second xys)]

    (testing "mean definition"
      (is-= (/ (sum first xys) n)
            (mean first xys)))

    (testing "linear regression and correlation"
      (is-approx-=
       {:mean-x mean-x
        :mean-y mean-y
        ;; sample variance
        :var-x (/ (sum (fn [[x y]] (square (- x mean-x))) xys) (dec n))
        :var-y (/ (sum (fn [[x y]] (square (- y mean-y))) xys) (dec n))
        :count n
        :beta0 -39.062
        :beta1 61.272
        :r-xy 0.9945837936}
       (bi-report (bi-stats xys))))))

(deftest windowed-stats-test
  (is-= (map double [1 (/ 3 2) 2 (/ 5 2) (/ 15 4) (/ 23 4) 10])
        (map #(uni-mean (:stats %))
             (next
              (reductions add-windowed-obs (windowed-stats 4)
                          [1 2 3 4 6 10 20])))))
