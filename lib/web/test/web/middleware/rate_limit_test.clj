(ns web.middleware.rate-limit-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [plumbing.expiring-cache :as expiring-cache]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [web.middleware.rate-limit :as rate-limit]))

(defn throw-it-all [& args]
  (log/throw+ {:args args}))

(deftest time-rate-limit-test
  (let [key "ip//a.d.d.r"
        rate-limit (fn [cache handler request & [success-count-atom failure-count-atom token-limit]]
                     (try
                       ((rate-limit/rate-limit-middleware
                         cache
                         (constantly (rate-limit/time-rate-limit key 200.0 (or token-limit 500.0) 1.0))
                         throw-it-all
                         handler)
                        {})
                       (when success-count-atom
                         (swap! success-count-atom inc))
                       (catch Exception e
                         (when failure-count-atom
                           (swap! failure-count-atom inc)))))]

    (testing "Three concurrent reqeust are called, only 2 of them pass"
      (let [cache (rate-limit/rate-limit-cache {})
            success-count (atom 0)
            failure-count (atom 0)
            handler (fn [_] (Thread/sleep 100))]
        (dotimes [_ 3]
          (future
            (rate-limit cache handler nil success-count failure-count)))
        (is-eventually (= @success-count 2))
        (is-eventually (= @failure-count 1))
        (resource/close cache)))

    (testing "singlethreaded should leave allowance where it started"
      (let [cache (rate-limit/rate-limit-cache {})
            handler (fn [_] (Thread/sleep 100))]
        (expiring-cache/put cache key {:allowance 200.0 :last-check (millis)})
        (rate-limit cache handler nil nil nil 500.0)
        (is (< 190.0 (:allowance (expiring-cache/get cache key)) 210.0))
        (resource/close cache)))

    (testing "Three concurrent reqeust are called, only 2 of them pass"
      (let [cache (rate-limit/rate-limit-cache {})
            success-count (atom 0)
            failure-count (atom 0)
            handler (fn [_] (Thread/sleep 100))]
        (dotimes [_ 4]
          (future
            (rate-limit cache handler nil success-count failure-count 1000.0)))
        (is-eventually (= @success-count 4))

        ;; Should be around these numbers
        (is (< 680 (:allowance (expiring-cache/get cache key)) 720))
        (resource/close cache)))))

(deftest always-allow-test
  (let [cache (rate-limit/rate-limit-cache {})
        success-atom (atom 0)
        h (rate-limit/rate-limit-middleware
           cache (constantly rate-limit/+no-rate-limit+)
           throw-it-all
           (fn [req] (swap! success-atom inc)))]
    (is (do (parallel/do-work 20 #(h {:num %}) (range 10000)) true))
    (is (= 10000 @success-atom))))

(deftest call-rate-limit-test
  (let [cache (rate-limit/rate-limit-cache {})
        success-atom (atom 0)
        h (rate-limit/rate-limit-middleware
           cache (constantly (rate-limit/call-rate-limit "abc" 100 1))
           (constantly {:fail true})
           (fn [req] (swap! success-atom inc)))]
    (is (do (parallel/do-work 20 #(h {:num %}) (range 1000)) true))
    (is (<= 100 @success-atom 120))))

(deftest ^:slow call-rate-limit-wait-test
  (let [cache (rate-limit/rate-limit-cache {})
        success-atom (atom 0)
        h (rate-limit/rate-limit-middleware
           cache (constantly (rate-limit/call-rate-limit "abc" 100 1))
           (constantly {:fail true})
           (fn [req] (swap! success-atom inc)))]
    (is (do (parallel/do-work 20 #(h {:num %}) (range 300)) true))
    (is (<= 100 @success-atom 120))
    (Thread/sleep 500)
    (is (do (parallel/do-work 20 #(h {:num %}) (range 300)) true))
    (is (<= 150 @success-atom 170))))
