(ns classify.online.moe-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [clojure.pprint :as pprint]
   [flop.distribution :as distribution]
   [flop.math :as math]
   [flop.stats :as stats]
   [classify.online.moe :as moe]))

(deftest negate-historical-info-test
  (is-= {:points_sampled
         [{:point [1] :value -1 :value_var 1}
          {:point [2] :value -2 :value_var 2}]}
        (moe/negate-historical-info
         {:points_sampled
          [{:point [1] :value 1 :value_var 1}
           {:point [2] :value 2 :value_var 2}]})))

(defn fake-stats [n mean var]
  (stats/->UnivariateStats
   (* mean n)
   (* n (+ (* mean mean) var))
   Double/NEGATIVE_INFINITY
   Double/POSITIVE_INFINITY
   n))

(deftest moe-arms-test
  (with-redefs [moe/mean-variance
                (fn [_ _ pts _]
                  {:mean (map #(safe-get {[1.0] 1.1 [2.0] 1.9 [2.5] 1.4} %) pts)
                   :var (map #(safe-get {[1.0] 0.5 [2.0] 0.05 [2.5] 1.2} %) pts)
                   :endpoint "gp_mean_var_diag"})
                moe/next-point-epi
                (constantly
                 {:points_to_sample [[2.5]]
                  :status "ok"
                  :endpoint "gp_next_points_epi"})]
    (let [fp #(distribution/fixed-posterior
               (distribution/gaussian %1 (Math/sqrt %2)))]
      (is-= {[1.0] (fp 1.1 0.5),
             [2.0] (fp 1.9 0.05)
             [2.5] (fp 1.4 1.2)}
            (moe/moe-arms
             nil nil 0.1
             {[1.0] (fake-stats 10 1.0 1.0)
              [2.0] (fake-stats 1000 2.0 1.0)})))))

(let [r (plumbing.MersenneTwister.)]
  (defn stats [n dist]
    (stats/uni-stats
     (repeatedly n #(distribution/sample dist r)))))

(defn test-variance-handling
  "Check that the means and variances out of MOE converge to the true
   variances as you add more data (i.e., we're not messing something
   up in the conversion to MOE and back)."
  [client]
  (let [means {[1.0] 1.0 [2.0] 2.0 [4.0] 1.0}]
    (doseq [n [3 10 100 1000 10000 100000 1000000]]
      (let [base-stats (map-vals #(fake-stats n % 1.0) means)
            uni-stats #(mapv (stats/uni-report %) [:mean :var])
            dist-info (fn->> (map-vals #(uni-stats (stats 100000 (distribution/posterior-mean-distribution %)))))]
        (println "\n\n" n "raw")
        (pprint/pprint (map-vals uni-stats base-stats))
        (println "pre-moe posterior mean")
        (pprint/pprint
         (dist-info
          (map-vals
           #(assoc (distribution/unknown-variance-gaussian 0.0)
              :uni-stats %)
           base-stats)))
        (println "post-moe posterior means")
        (pprint/pprint
         (dist-info
          (select-keys
           (moe/moe-arms
            client
            {:domain_info (moe/domain-info [{:min 0.0 :max 5.0}])
             :covariance_info {:covariance_type "square_exponential"
                               :hyperparameters [1 2.0]}
             :optimizer_info {:optimizer_type "gradient_descent_optimizer"
                              :num_multistarts 50
                              :num_random_samples 400
                              :optimizer_parameters {:max_num_steps 300}}}
            0.1
            base-stats)
           (keys means))))))))
