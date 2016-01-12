(ns store.mongo-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [store.bucket :as bucket]
   [store.mongo :as mongo]))


(defn bucket-test [s]
  (let [b (mongo/bucket-collection s "testBucket")]
    (bucket/put b "k1" {:v "v1"})
    (is (= (bucket/get b "k1") {:v "v1"}))
    (is (= 1 (count (filter (partial = "k1") (bucket/keys b)))))
    (is (bucket/exists? b "k1"))
    (bucket/delete b "k1")
    (is (not (bucket/exists? b "k1")))
    (bucket/put b "k2" {:a 1})
    (is (= 1 (-> b (bucket/get "k2") :a)))
    (bucket/put b "k2" {:a 2})
    (is (= 2 (:a (bucket/get b "k2"))))
    (is (= [["k2",{:a 2}]] (bucket/seq b)))
    (is (nil? (bucket/get b "dne")))
    (bucket/update b "k2" #(update-in % [:a] inc))
    (is (= (:a (bucket/get b "k2")) 3))
    (let [batch {"k3" {:v 3}
                 "k4" {:v 4}
                 "k5" {:v 5}}]
      (bucket/batch-put b batch)
      (is (= batch (into {} (bucket/batch-get b [ "k3"
                                                  "k4"
                                                  "k5"])))))
    (mongo/drop-collection s "testBucket")))

(defn coll-seq [s n]
  (let [d (bucket/seq (mongo/bucket-collection s n))]
    (map second
         (if (number? (ffirst d)) ;; account for store
           (sort-by first d)
           d))))

(defn capped-test [s]
  (let [c (mongo/capped-collection s "testCapped" {:max-mb 1 :max-count 3})
        data [{:a 1 :b 2} {:b 6 :a 2} {:c 4} {:e 12} {:z "test"}]]
    (dotimes [i (count data)]
      (mongo/append c (nth data i))
      (is (= (take-last 3 (take (inc i) data)) (coll-seq s "testCapped"))))
    (mongo/drop-collection s "testCapped")))

(defn aggregate-test [s]
  (testing "update with sets and incs"
    (let [a (mongo/aggregate-collection s "testAgg" {:agg-keys [:k1 :k2]})
          docs (fn [] (set (coll-seq s "testAgg")))]
      (is (= #{} (docs)))
      (mongo/update a {:k1 1 :k2 2 :k3 3} {:v1 1} {:v2 3 :v3 5})
      (is (= #{{:k1 1 :k2 2 :v1 1 :v2 3 :v3 5}}
             (docs)))
      (mongo/update a {:k1 1} {:v1 4} {:v2 2})
      (mongo/update a {:k1 1 :k2 2 :k3 6 :k4 8} {:v2 100 :v7 9} {:v3 5})


      (is (= #{{:k1 1 :k2 2 :v1 1 :v2 100 :v3 10 :v7 9}
               {:k1 1 :k2 nil :v1 4 :v2 2}}
             (docs)))
      (mongo/drop-collection s "testAgg")))

  (testing "update with raw-ops"
    (let [a (mongo/aggregate-collection s "testAgg" {:agg-keys [:k1 :k2]})
          docs (fn [] (set (coll-seq s "testAgg")))]
      (is-= #{} (docs))
      (mongo/update a {:k1 1 :k2 2} {:v1 1 :v2 0 :v3 #{:a} :v4 [:c]} {})
      (is-= #{{:k1 1 :k2 2 :v1 1 :v2 0 :v3 #{:a} :v4 [:c]}} (docs))
      (mongo/update a {:k1 1 :k2 2} {"$inc" {:v1 1}})
      (mongo/update a {:k1 1 :k2 2} {"$set" {:v2 1}})
      (mongo/update a {:k1 1 :k2 2} {"$addToSet" {:v3 :b}})
      (mongo/update a {:k1 1 :k2 2} {"$push" {:v4 :d}})
      (is-= #{{:k1 1 :k2 2 :v1 2 :v2 1 :v3 #{:a :b} :v4 [:c :d]}} (docs)))))

(defn test-log-data-store [s]
  (bucket-test s)
  (capped-test s)
  (aggregate-test s))

(deftest mem-store-test
  (test-log-data-store (mongo/test-log-data-store)))
