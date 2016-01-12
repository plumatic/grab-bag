(ns plumbing.expiring-cache-test
  (:use clojure.test plumbing.test)
  (:require
   [plumbing.expiring-cache :as ec]
   [plumbing.core :as plumbing]
   [plumbing.resource :as resource]
   ))

(deftest always-prune-test
  (let [ec (ec/create 0 0)]
    (is (nil? (ec/get ec 1)))
    (is (= (ec/count ec) 0))
    (is (= 2 (ec/get ec 1 inc)))
    (is (= (ec/count ec) 1))
    (is (= 3 (ec/get ec 2 inc)))
    (is (= (ec/count ec) 1))
    (is (= 2 (ec/get ec 1 inc)))
    (is (= (ec/count ec) 1))
    (is (nil? (ec/get ec 1)))
    (is (= (ec/count ec) 0))))

(deftest never-prune-test
  (let [ec (ec/create 1000 0)]
    (is (nil? (ec/get ec 1)))
    (is (= (ec/count ec) 0))
    (is (= 2 (ec/get ec 1 inc)))
    (is (= (ec/count ec) 1))
    (is (= 2 (ec/get ec 1)))
    (is (= 2 (ec/get ec 1 :dont-call-me)))
    (is (= 10 (ec/get ec 11 dec)))
    (is (= :foo (ec/get ec :foo identity)))
    (is (= (ec/count ec) 3))
    (is (= 10 (ec/get ec 11 inc)))
    (is (= 10 (ec/get ec 11)))
    (is (= 42 (ec/put ec 11 42)))
    (is (= 42 (ec/get ec 11)))
    (is (= 2 (ec/get ec 1)))))


(deftest delayed-prune-test
  (let [ec (with-millis 0 (ec/create 50 1))]
    (with-millis 0
      (doseq [i (range 10)]
        (is (= (inc i) (ec/get ec i inc)))))
    (with-millis 25
      (doseq [i (range 8 20)]
        (is (= ((if (< i 10) inc dec) i) (ec/get ec i dec))))
      (is (= :zorb (ec/put ec 0 :zorb)))
      (is (= :chinz (ec/put ec 20 :chinz))))
    (with-millis 51
      (is (= (ec/count ec) 21))
      (is (nil? (ec/get ec 1)))
      (is (= (ec/count ec) 12))
      (doseq [i (range 10 20)]
        (is (= (ec/get ec i) (dec i))))
      (is (= (ec/get ec 0 :zorb)))
      (is (= (ec/get ec 0 :chinz))))))

(deftest ^:slow concurrent-test
  (let [expired (atom [])
        ec (ec/create-concurrent 5000 1000 true (partial swap! expired conj))]
    (try
      (ec/update ec "doesn't exist yet" identity)
      (is (= 0 (ec/count ec)))
      (doseq [i (range 10)]
        (is (= (inc i) (ec/get ec i inc))))
      (is (empty? @expired))

      (Thread/sleep 2000)
      (doseq [i (range 8 20)]
        (is (= ((if (< i 10) inc dec) i) (ec/get ec i dec))))
      (is (= :zorb (ec/put ec 0 :zorb)))
      (is (= :kittens (ec/put ec 20 :kittens)))

      (Thread/sleep 4000)

      (is (= (ec/count ec) 14))
      (is (= (set @expired) (set (for [i (range 1 8)] [i (inc i)]))))
      (is (nil? (ec/get ec 1)))
      (doseq [i (range 10 20)]
        (is (= (ec/get ec i) (dec i))))
      (is (= (ec/get ec 0 :zorb)))
      (ec/update ec 0 (fn [zorb] :not-zorb))
      (is (= :not-zorb (ec/get ec 0)))
      (is (= (ec/get ec 0 :kittens)))
      (finally (resource/close ec)))
    (is (= (count @expired) 21))
    ))
