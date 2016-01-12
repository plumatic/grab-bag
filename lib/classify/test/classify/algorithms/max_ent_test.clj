(ns classify.algorithms.max-ent-test
  (:use clojure.test plumbing.core plumbing.test classify.algorithms.max-ent)
  (:require
   [plumbing.index :as plumbing-index]
   [plumbing.io :as io]
   [flop.map :as map]
   [flop.empirical-gradient :as empirical-gradient]
   [classify.core :as classify]))

(deftest unsplat-test
  (let [p-index (plumbing-index/static [:a :b :c])
        l-index (plumbing-index/static [:a :b :c])
        weight-arr (double-array (range 9))
        unsplatted (unsplat weight-arr l-index p-index false)]
    (is (= (map-vals (comp second second io/to-data) unsplatted)
           {:a [0.0 3.0 6.0]
            :b [1.0 4.0 7.0]
            :c [2.0 5.0 8.0]}))
    (let [p-index (plumbing-index/static [:a :b :c])
          l-index (plumbing-index/static [:a :b])
          weight-arr (double-array (range 3))
          unsplatted (unsplat weight-arr l-index p-index false)]
      (is (= (map-vals (comp second second io/to-data) unsplatted)
             {:a [0.0 1.0 2.0]
              :b nil})))))

(deftest train-test
  (doseq [[data-maps pred-map weights best-label]
          [[[[{0 1.0 1 1.0} 0] [{1 1.0 2 1.0} 1]]
            {0 0.1 1 0.5 2 1.0}
            {0 [0.5671818574113499 0.0 -0.56718185741135], 1 []}
            1
            ]
           [[[{0 1.0 1 1.0} 0] [{1 1.0 2 1.0} 1] [{0 1.0 2 1.0} 1]]
            {0 1.0 1 0.5 2 0.0}
            {0 [0.22635838169146089 0.22635838169146089 -1.3319361434617065], 1 []}
            0]
           [[[{0 1.0 1 1.0} 0] [{1 1.0 2 1.0} 1] [{3 1.0 4 1.0} 2]]
            {0 1.0 1 0.5 2 0.0}
            {0 [0.8172447043699287 0.30713743293813134 -0.5101072714317971 -0.34028386504337094 -0.34028386504337094],
             1 [-0.5101072714317971 0.30713743293813134 0.8172447043699287 -0.34028386504337094 -0.34028386504337094],
             2 [-0.30713973477063733 -0.6142794695412747 -0.30713973477063733 0.6805539261096829 0.6805539261096829]}
            0]
           [[[{0 1.0 1 1.0} 0 1.0] [{1 1.0 2 1.0} 1 2.0]]
            {0 0.1 1 0.5 2 1.0}
            {0 [0.5671818574113499 0.0 -0.56718185741135], 1 []}
            1]]]
    (let [data (for [[d v] data-maps] [(map/map->fv d) v])
          classifier ((trainer {:normalize? true :unpack? true :sigma-sq (* 1 (count data))}) data)]
      (is-approx-= weights
                   (map-vals (fn->> io/to-data second (sort-by key) (map val))
                             (:label->weights classifier))
                   1.0e-9)
      (is (= best-label (classify/best-guess classifier (map/map->fv pred-map)))))))


(deftest bias-train-test
  (doseq [[data-maps pred-map weights best-label bias-f]
          [[[[{0 1.0 1 1.0} 0] [{1 1.0 2 1.0} 1]]
            {0 0.1 1 0.5 2 1.0}
            {0 [0.6748316143423994 0.0 -0.6748316143423994], 1 []}
            1]
           [[[{0 10.0 1 1.0} 0] [{0 9.0 1 1.0 2 1.0} 1]]
            {0 0.1 1 0.5 2 1.0}
            {0 [0.05968214368006695 -0.07619199771545322 -0.8216021208345976], 1 []}
            1]
           [[[{0 10.0 1 1.0} 0] [{0 9.0 1 1.0 2 1.0} 1]]
            {0 0.1 1 0.5 2 1.0}
                                        ;            {0 [ 0.40283199621730925 (* 9 -0.40283199621730925) -0.40283199621730925], 1 []} almost
            ;; ideally, we should hit (x (* -9 x) y) if (x 0 y) is the solution to the first one,
            ;; since we can use the unreguarized bias to correct for the +10 offset on feature 0.
            {0 [0.6748316122560268 -6.073484510148548 -0.6748316147757244], 1 []}
            1
            1]]]
    (let [data (for [[d v] data-maps] [(map/map->fv d) v])
          classifier ((trainer {:print-progress true :normalize? false :unpack? true :sigma-sq-fn #(if (= bias-f %) Double/POSITIVE_INFINITY (count data))}) data)]
      (is-approx-= weights
                   (map-vals (fn->> io/to-data second (sort-by key) (map val)) (:label->weights classifier))
                   1.0e-9)
      (is (= best-label (classify/best-guess classifier (map/map->fv pred-map)))))))

(def +train-data+
  [[[[{0 1.0 1 1.0} 0] [{1 1.0 2 1.0} 1]] ;; same as above
    {0 0.0 1 0.0 2 0.0} {0 2.0 1 2.0 2 2.0}
    {0 [0.6748316143423994 0.0 -0.6748316143423994], 1 []}]
   [[[{0 1.0 1 1.0} 0] [{1 1.0 2 1.0} 1]] ;; same as above with prior on weights
    {0 1.0 1 0.0 2 -1.0} {0 2.0 1 2.0 2 2.0}
    {0 [1.3966852630509494 0.0 -1.3966852630509494], 1 []}]])

(deftest mean-train-test
  (doseq [[data-maps weight-mean weight-ss weights] +train-data+]
    (let [data (for [[d v] data-maps] [(map/map->fv d) v])
          classifier ((trainer {:print-progress true :normalize? false :unpack? true
                                :prior-weights (flop.weight-vector/map->sparse weight-mean)
                                :sigma-sq-fn weight-ss}) data)]
      (is-approx-= weights
                   (map-vals (fn->> io/to-data second (sort-by key) (map val)) (:label->weights classifier))
                   1.0e-7))))

(deftest objective-gradient-consistency-test
  (letk [[p-index l-index i-data] (index-data {:data (ffirst +train-data+)})]
    (let [num-dims (count p-index)
          helper (new-helper num-dims (count l-index))
          f (obj-fn i-data helper nil 1)
          gradients (empirical-gradient/empirical-gradient {:num-dims 3 :f f})]
      (doseq [[grad emp-grad] gradients]
        (is-approx-= grad emp-grad 1e-4)))))

(use-fixtures :once validate-schemas)
