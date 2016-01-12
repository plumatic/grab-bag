(ns classify.online-test
  (:use clojure.test plumbing.core)
  (:require
   [plumbing.core-incubator :as pci]
   [plumbing.math :as math]
   [flop.math :as flop-math]
   [flop.distribution :as distribution]
   [classify.online :as online]
   [classify.online.policies :as policies]))

(defn test-setup
  "Return [env models]"
  [n]
  (let [r (plumbing.MersenneTwister. 12345)
        arms {:a (distribution/->Uniform 1 5)
              :b (distribution/->Gaussian 3.5 2)
              :c (distribution/->Gaussian 3 5)}]
    [(online/classical-bandit-environment r arms n)
     (map-vals
      (fn [prior] (policies/thompson-bandit r (keys arms) prior))
      {:known (distribution/known-variance-gaussian 0 1)
       :unknown (distribution/unknown-variance-gaussian 0)})]))

(deftest simple-bandit-operation
  (let [[env policies] (test-setup 50000)
        stats (->> (online/all-epoch-stats
                    env
                    (online/replicate-policy 10 (:unknown policies))
                    online/discrete-action-stats-fn 1000)
                   vals
                   (apply map vector)
                   (map (fn->> (apply pci/merge-all-with #(math/mean %&) 0.0))))]
    (testing "initially, all arms get some pulls."
      (doseq [arm [:a :b :c]]
        (testing arm
          (is (<= 0.02 (safe-get (first stats) arm))))))

    (testing "eventually, arm B gets nearly all pulls"
      (is (<= 0.98 (safe-get (last stats) :b) 1.00)))))


(defn sinusoidal-test-environment
  "An environment for predicting reserve on a single bid whose mean varies periodically.
   Algorithms that take non-stationarity into account should work better than, e.g.,
   standard bandit algorithms."
  [^java.util.Random r ^long n ^double a ^double p]
  (online/sequential-environment
   (for [i (range n)]
     (let [date (* i 1000)]
       [{:date date}
        (+ (* a (- 1 (Math/cos (/ (* 2.0 Math/PI i) 60.0 p))))
           (flop-math/sample-gaussian r))]))
   first
   :positive-real
   (fn [[_ bid] reserve]
     (if (< reserve bid) reserve 0))))

(deftest decayed-bandit-test
  (let [period 1000
        env (sinusoidal-test-environment (plumbing.MersenneTwister. 123) (* period 60) 1 period)
        total-reward (fn [bandit]
                       (sum :reward-mean (online/epoch-stats env bandit online/continuous-stats-fn (* period 2))))]
    (is (> (total-reward
            (policies/decaying-thompson-bandit
             (plumbing.MersenneTwister. 123)
             (range 0 2.0 0.2)
             (distribution/unknown-variance-gaussian 0.0)
             0.99))
           15.0))
    (is (< (total-reward
            (policies/thompson-bandit
             (plumbing.MersenneTwister. 123)
             (range 0 2.0 0.2)
             (distribution/unknown-variance-gaussian 0.0)))
           14.0))))
