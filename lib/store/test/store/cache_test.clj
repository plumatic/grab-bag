(ns store.cache-test
  (:use plumbing.core clojure.test plumbing.test)
  (:require
   [store.bucket :as bucket]
   [store.cache :as cache]))

(deftest expiring-bucket-wrapper-test
  (let [b (cache/expiring-bucket-wrapper (bucket/bucket {}) 50)]
    (with-millis 0
      (doseq [i (range 10)]
        (is (= (inc i) (cache/cache b inc i)))))
    (with-millis 25
      (doseq [i (range 8 20)]
        (is (= ((if (< i 10) inc dec) i) (cache/cache b dec i))))
      (bucket/put b 0 :zorb)
      (bucket/put b 20 :chinz))
    (with-millis 51
      (is (= (bucket/count b) 21))
      (is (nil? (bucket/get b 1)))
      (is (= 9 (bucket/get b 10)))
      (is (= (bucket/count b) 20))
      (bucket/sync b)
      (is (= (bucket/count b) 12))
      (doseq [i (range 10 20)]
        (is (= (bucket/get b i) (dec i))))
      (is (= (bucket/get b 0) :zorb))
      (is (= (bucket/get b 20) :chinz))
      (bucket/update b 1 (fn [x] (if x [x] 17)))
      (bucket/update b 1 (fn [x] (if x [x] 17)))
      (bucket/update b 12 inc)
      (bucket/update b 12 inc))
    (with-millis 100
      (is (= (into {} (bucket/seq b))
             {1 [17] 12 13})))
    (with-millis 105
      (is (empty? (bucket/vals b)))
      (is (= 0 (bucket/count b))))))