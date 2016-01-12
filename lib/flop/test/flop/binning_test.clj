(ns flop.binning-test
  (:require
   [plumbing.core :refer :all]
   [plumbing.test :refer :all]
   [flop.binning :refer :all]
   [clojure.test :refer :all]
   [flop.stats :as stats]))

(deftest cum-sum-test
  (let [cum-sum-keys (fn [seqs]
                       (->>
                        seqs
                        (map stats/uni-stats)
                        cum-sum
                        (map #(select-keys % [:num-obs :sum-xs :sum-sq-xs]))))]

    (is-= [{:num-obs 0.0 :sum-xs 0.0 :sum-sq-xs 0.0}
           {:num-obs 1.0 :sum-xs 1.0 :sum-sq-xs 1.0}]
          (cum-sum-keys [[1]]))

    (is-= [{:num-obs 0.0 :sum-xs 0.0 :sum-sq-xs 0.0}
           {:num-obs 1.0 :sum-xs 2.0 :sum-sq-xs 4.0}
           {:num-obs 4.0 :sum-xs 32.0 :sum-sq-xs 304.0}]
          (cum-sum-keys
           [[2]
            [10 10 10]]))

    (is-= [{:num-obs 0.0 :sum-xs 0.0 :sum-sq-xs 0.0}
           {:num-obs 1.0 :sum-xs 1.0 :sum-sq-xs 1.0}
           {:num-obs 5.0 :sum-xs 9.0 :sum-sq-xs 17.0}
           {:num-obs 10.0 :sum-xs 24.0 :sum-sq-xs 62.0}]
          (cum-sum-keys
           [[1]
            [2 2 2 2]
            [3 3 3 3 3]]))))

(deftest distortion-test
  (is-= 0.0 (distortion 3 0 0))
  (is-= 0.0 (distortion 1 2 4))
  (is-= 1.0 (distortion 4 2 2)))

(deftest span-distortion-test
  (let [cum-sums (->> [[2] [0 0 1 1]]
                      (map stats/uni-stats)
                      cum-sum)]

    (is-= (distortion 1 2 4)
          (span-distortion cum-sums 0 1))

    (is-= (distortion 4 2 2)
          (span-distortion cum-sums 1 2))

    (is-= (distortion 5 4 6)
          (span-distortion cum-sums 0 2)))

  (let [n 30
        nums (repeatedly n #(rand-int 100))
        stats (mapv (comp stats/uni-stats vector) nums)
        cum-sums (cum-sum stats)]
    (doseq [i (range n)
            j (range (inc i) n)]
      (testing (format "span[%s, %s)" i j)
        (is-= (let [slice (subvec stats i j)]
                (distortion
                 (sum :num-obs slice)
                 (sum :sum-xs slice)
                 (sum :sum-sq-xs slice)))
              (span-distortion cum-sums i j))))))

(defn partitions
  "partition seq x into k pieces for brute force testing"
  [k x]
  (if (= 1 k)
    [[x]]
    (for [i (range 1 (count x))
          :let [[left right] (split-at i x)]
          rec (partitions (dec k) left)]
      (conj (vec rec) right))))

(deftest partitions-test
  (testing "only 1 way to partition 2 items into 2 groups"
    (is-= [[[1] [2]]]
          (partitions 2 [1 2])))

  (testing "6 ways to partition 5 items into 3 groups"
    (is-= [[[1] [2] [3 4 5]]
           [[1] [2 3] [4 5]]
           [[1 2] [3] [4 5]]
           [[1] [2 3 4] [5]]
           [[1 2] [3 4] [5]]
           [[1 2 3] [4] [5]]]
          (partitions 3 [1 2 3 4 5]))))

(deftest opt-bins-test
  (let [to-stats (fn [raw-s] (map (comp stats/uni-stats vector) raw-s))
        canonical (fn [res] (select-keys res [:score :indices]))]

    (testing "some small examples"
      (is-= 0.0 (:score (opt-bins 3 (to-stats (repeat 100 100)))))

      (is-= {:score 0.0 :indices [2]}
            (canonical (opt-bins 2 (to-stats [0 0 1 1]))))

      (is-= {:score 0.0 :indices [3 6]}
            (canonical (opt-bins 3 (to-stats [0 0 0 1 1 1 2 2 2])))))

    (testing "brute force bigger examples"
      (let [score-partition (fn [partition]
                              (sum
                               (fn [slice]
                                 (distortion
                                  (sum :num-obs slice)
                                  (sum :sum-xs slice)
                                  (sum :sum-sq-xs slice)))
                               partition))

            brute-force-best (fn [k vs]
                               (let [partition
                                     (->> vs
                                          (partitions k)
                                          (apply min-key score-partition))]
                                 {:score (score-partition partition)
                                  :indices (->> partition
                                                drop-last
                                                (map count)
                                                (reductions +))}))
            n 13]
        (doseq [k (range 2 7)]
          (let [nums (to-stats (repeatedly n #(rand)))]
            (testing (str "nums: " (vec nums))
              (let [res (opt-bins k nums)]
                (letk [[score indices] (brute-force-best k nums)]
                  (is-approx-= score (:score res) 1.0e-8)
                  (is-= indices (:indices res)))))))))))
