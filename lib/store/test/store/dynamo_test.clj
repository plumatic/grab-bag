(ns store.dynamo-test
  (:use clojure.test store.dynamo plumbing.test)
  (:require
   [aws.dynamo :as dynamo]
   [store.bucket :as bucket])
  (:import
   [com.amazonaws.services.dynamodbv2.model ProvisionedThroughputExceededException]))

;; differs from usual bucket test in the use of make-k fn.
(defn generic-bucket-test [b make-k]
  (let [k1 (make-k "k" 1)
        v1 {:val1 "v1" :val2 2}
        k2 (make-k "k" 2)
        v2 {:val5 "v7" :val0 0.5}
        k3 (make-k "r" 1)
        v3 {:val1 "v2" :val2 -2}]
    (bucket/put b k1 v1)
    (is (= (bucket/get b k1) v1))
    (is (= [k1] (bucket/keys b)))
    (is (= [[k1 v1]] (bucket/seq b)))
    (is (bucket/exists? b k1))
    (is (not (bucket/exists? b k2)))
    (bucket/put b k2 v2)
    (bucket/put b k3 v3)
    (is (bucket/exists? b k3))
    (is (= #{k1 k2 k3} (set (bucket/keys b))))
    (is (= {k1 v1 k2 v2 k3 v3} (into {} (bucket/seq b))))
    (bucket/clear b)
    (is (empty? (bucket/seq b)))))


;; Takes close to a minute to delete a table, so don't try to do these in the test.

(defn setup-throughput [ec2-keys]
  (dynamo/with-dynamo-client [c ec2-keys]
    (dynamo/create-table {:client c :name "test-throughput" :write-throughput 1 :read-throughput 1
                          :hash-key [:kittens dynamo/+string+]})))

(defn hammer-city [ec2-keys]
  (let [b1 (bucket/bucket (merge ec2-keys {:type :dynamo :name "test-throughput"}))]
    (dotimes [i 1000]
      (bucket/put b1 [(str i)] {:data (byte-array 32000)}))))

(defn teardown-throughput [ec2-keys]
  (dynamo/with-dynamo-client [c ec2-keys]
    (dynamo/delete-table {:client c :name "test-throughput"})))




(defn setup [ec2-keys]
  (dynamo/with-dynamo-client [c ec2-keys]
    (dynamo/create-table {:client c :name "test-table1" :write-throughput 5 :read-throughput 3
                          :hash-key [:kittens dynamo/+string+]})
    (dynamo/create-table {:client c :name "test-table2" :write-throughput 5 :read-throughput 3
                          :hash-key [:kittens dynamo/+string+] :range-key [:face dynamo/+number+]})    (let [c (dynamo/dynamo-client ec2-keys)])))

(deftest dynamo-method-test
  (testing "normal case"
    (is-= 1 (dynamo-method "test-1" (constantly 1) 1)))

  (testing "recovers if fewer than max-attempt errors"
    (let [failures (atom 0)
          max-fail 2
          f #(if (= @failures (dec max-fail))
               1
               (do
                 (swap! failures inc)
                 (throw (ProvisionedThroughputExceededException. "duh"))))]
      (is-= 1 (dynamo-method "test-2" f max-fail))
      (is-= 1 @failures)))

  (testing "throws exception if max-attempts exceeded"
    (let [max-fail 2
          f #(throw (ProvisionedThroughputExceededException. "noooo!"))]
      (is (thrown? ProvisionedThroughputExceededException
                   (dynamo-method "test-3" f max-fail))))))

(defn test-simple-bucket [ec2-keys serialize-method binary?]
  (println "testing" "simple")
  (let [b1 (bucket/bucket (merge ec2-keys {:type :dynamo :name "test-table1" :serialize-method serialize-method :binary? binary?}))
        b2 (bucket/bucket (merge ec2-keys {:type :dynamo :name "test-table2" :serialize-method serialize-method :binary? binary?}))]
    (try
      (generic-bucket-test b1 (fn [p1 p2] [(str p1 p2)]))
      (generic-bucket-test b2 (fn [p1 p2] [p1 p2]))
      (finally
        (bucket/clear b1) (bucket/clear b2)
        (bucket/close b1) (bucket/close b2)))))

(defn test-range-queries* [b2]
  (let [data {["a" 3] {:v "a3"}
              ["a" 1] {:v "a1"}
              ["a" 5] {:v "a5"}
              ["b" 1] {:v "b1"}
              ["b" 2] {:v "b2"}
              ["a" 7] {:v "a7"}}
        simple-query (comp first simple-query)]
    (doseq [[k v] data]
      (bucket/put b2 k v))

    (is (empty? (simple-query b2 "c" nil false 10)))

    (is-= (simple-query b2 "a" nil false 10)
          (sort-by (comp second first) (filter #(= (ffirst %) "a") data)))
    (is-= (simple-query b2 "a" nil true 10)
          (sort-by (comp - second first) (filter #(= (ffirst %) "a") data)))

    (is-= (simple-query b2 "a" nil false 2)
          (take 2 (sort-by (comp second first) (filter #(= (ffirst %) "a") data))))
    (is-= (simple-query b2 "a" nil true 2)
          (take 2 (sort-by (comp - second first) (filter #(= (ffirst %) "a") data))))

    (is-= (simple-query b2 "a" 2 false 10)
          (drop 1 (sort-by (comp second first) (filter #(= (ffirst %) "a") data))))
    (is-= (simple-query b2 "a" 6 true 10)
          (drop 1 (sort-by (comp - second first) (filter #(= (ffirst %) "a") data))))

    (is-= (simple-query b2 "a" 3 false 10)
          (drop 2 (sort-by (comp second first) (filter #(= (ffirst %) "a") data))))
    (is-= (simple-query b2 "a" 5 true 10)
          (drop 2 (sort-by (comp - second first) (filter #(= (ffirst %) "a") data))))

    (is-= (simple-query b2 "a" 2 false 1)
          [[["a" 3] {:v "a3"}]])

    (bucket/clear b2)
    (is (empty? (bucket/seq b2)))))

(defn test-range-queries [ec2-keys serialize-method binary?]
  (println "testing" "range")
  (let [b2 (bucket/bucket (merge ec2-keys {:type :dynamo :name "test-table2" :serialize-method serialize-method :binary? binary?}))]
    (try
      (test-range-queries* b2)
      (finally (bucket/close b2)))))

(defn test-binary-serialization [ec2-keys]
  (let [b1 (bucket/bucket (merge ec2-keys {:type :dynamo :name "test-table1" :serialize-method plumbing.serialize/+clojure+ :binary? true}))
        b2 (bucket/bucket (merge ec2-keys {:type :dynamo :name "test-table1" :serialize-method nil}))]
    (try
      (bucket/put b1 ["a"] [1])
      (is (= (seq (:data (bucket/get b2 ["a"])))
             (cons plumbing.serialize/+clojure+ (.getBytes "[1]" "UTF-8"))))
      (finally
        (bucket/clear b1) (bucket/clear b2)
        (bucket/close b1) (bucket/close b2)))))

(defn dynamo-test [ec2-keys]
  (dynamo/with-dynamo-client [c ec2-keys]
    (is (clojure.set/subset? #{"test-table1" "test-table2"} (set (dynamo/list-tables c))))
    (doseq [[t b] [[nil] [:clj] [plumbing.serialize/+default+] [plumbing.serialize/+default+ true]]]
      (println (format "testing %s %s" t b))
      (test-simple-bucket ec2-keys t b)
      (test-range-queries ec2-keys t b))
    (test-binary-serialization ec2-keys)))

(defn teardown [ec2-keys]
  (dynamo/with-dynamo-client [c ec2-keys]
    (dynamo/delete-table {:client c :name "test-table1"})
    (dynamo/delete-table {:client c :name "test-table2"})))

(deftest mem-bucket-query-test
  (test-range-queries* (bucket/bucket {})))

(deftest ^:slow atomic-update-test
  (let [b (bucket/bucket {:type :mem})
        k "test-key"
        n 100
        slow-inc (fn [n] (Thread/sleep (rand-int 10)) (inc n))
        update-futures (doall (for [i (range n)]
                                (future (atomic-update b k (fnil slow-inc 0)))))]
    (doseq [update-future update-futures]
      @update-future)
    (is-= n (bucket/get b k))))
