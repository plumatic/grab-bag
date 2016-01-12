(ns store.bucket-test
  (:use clojure.test plumbing.test)
  (:require [plumbing.io :as io] [store.bucket :as bucket]))

(defn generic-bucket-test [b]
  (bucket/put b "k1" "v1")
  (is (= (bucket/get b "k1") "v1"))
  (is (= 1 (count (filter (partial = "k1") (bucket/keys b)))))
  (is (bucket/exists? b "k1"))
  (bucket/delete b "k1")
  (is (not (bucket/exists? b "k1")))
  (bucket/put b "k2" {:a 1})
  (is (= 1 (-> b (bucket/get "k2") :a)))
  (bucket/put b "k2" 2)
  (is (= 2 (bucket/get b "k2")))
  (is (= [["k2",2]] (bucket/seq b)))
  (is (nil? (bucket/get b "dne")))
  (bucket/update b "k2" inc)
  (is (= (bucket/get b "k2") 3))
  (let [batch {"k3" 3
               "k4" 4
               "k5" 5}]
    (bucket/batch-put b batch)
    (is (= batch (into {} (bucket/batch-get b [ "k3"
                                                "k4"
                                                "k5"])))))

  (testing "get!"
    (let [misses (atom 0)
          default-expression #(do (swap! misses inc) "default")]
      (is (not (bucket/exists? b "k1")))
      (is (not (bucket/exists? b "k6")))
      (is-= "default" (bucket/get! b "k1" (default-expression)))
      (is-= 1 @misses)
      (is-= "default" (bucket/get! b "k1" (default-expression)))
      (is-= 1 @misses)
      (is-= "default" (bucket/get! b "k6" (default-expression)))
      (is-= 2 @misses)))
  (bucket/clear b))

(deftest mem-bucket-test
  (generic-bucket-test (bucket/bucket {:type :mem}))
  (let [b (bucket/bucket {:type :mem})]
    (bucket/update b "k7" (constantly nil))
    (is (empty? (bucket/seq b)))
    (bucket/update b "k7" (constantly :foo))
    (is (= [["k7" :foo]] (bucket/seq b)))
    (bucket/update b "k7" (constantly nil))
    (is (empty? (bucket/seq b)))
    (bucket/put b :k1 :v1)
    (bucket/put b :k2 :v2)
    (is (= #{:v1 :v2} (set (bucket/vals b))))))

(deftest update-in-test
  (let [b (bucket/bucket {:type :mem})]
    (bucket/update-in b [:foo :bar] (constantly "test"))
    (is (= [[:foo {:bar "test"}]] (bucket/seq b)))
    (bucket/update-in b [:foo] (constantly "pwned"))
    (is (= [[:foo "pwned"]] (bucket/seq b)))))

(deftest get-in-test
  (let [b (bucket/bucket {:type :mem})]
    (bucket/put b :foo {:bar "test"})
    (is (= "test" (bucket/get-in b [:foo :bar])))
    (is (= {:bar "test"} (bucket/get-in b [:foo])))))

(deftest fs-bucket-test
  (io/with-test-dir [p "/tmp/store-core-test"]
    (generic-bucket-test
     (bucket/bucket {:type :fs
                     :name "foo"
                     :path p}))))

(defn merge-to!
  "merge takes (k to-value from-value)"
  [from to]
  (doseq [[k v] (bucket/seq from)]
    (bucket/merge to k v))
  to)

(deftest flush-test
  (let [b1 (bucket/bucket {:type :mem})
        b2 (bucket/bucket {:type :mem :merge (fn [_ v1 v2] (merge v1 v2))})]
    (bucket/put b1 :foo {:bar "bar"})
    (bucket/put b1 :nutty {:mcsackhang "mcsackhang"})
    (merge-to! b1 b2)
    (is (= (into {} (bucket/seq b2))
           (into {} (bucket/seq b1))))
    (merge-to! {:bar {:kittens "deep"}} b2)
    (is (= {:bar {:kittens "deep"}
            :foo {:bar "bar"}
            :nutty {:mcsackhang "mcsackhang"}}
           (into {} (bucket/seq b2))))))

(deftest with-flush-test
  (let [merge-fn (fnil (fn [_ v1 v2] (merge v1 v2)) {})
        b1 (bucket/bucket {:type :mem :merge merge-fn})
        b2 (bucket/with-flush b1 merge-fn)]
    (bucket/merge b2 "k" {"v1" "v"})
    (is (nil? (bucket/get b1 "k")))
    (bucket/sync b2)
    (is (= (bucket/get b1 "k") {"v1" "v"}))
    (is (= {"v1" "v"} (bucket/delete b2 "k")))
    (bucket/sync b2)
    (is (nil? (bucket/get b2 "k")))))

(deftest bucket-counting-merge-test
  (let [n 1000
        b (bucket/bucket {:type :mem :merge (fn [_ sum x] (+ (or sum 0) x))})
        latch (java.util.concurrent.CountDownLatch. n)
        pool (java.util.concurrent.Executors/newFixedThreadPool 10)]
    (dotimes [_ n]
      (.submit pool (cast Runnable (fn []
                                     (bucket/merge b "k" 1)
                                     (.countDown latch)))))
    (.await latch)
    (bucket/get b "k")))

(deftest write-through-mem-cache-test
  (let [op-counts (atom {})
        v-atom (atom 1)
        underlying (reify
                     store.bucket.IReadBucket
                     (get [this k] (swap! op-counts update-in [:get] (fnil inc 0))
                       @v-atom)
                     store.bucket.IWriteBucket
                     (put [this k v] (swap! op-counts update-in [:put] (fnil inc 0))
                       (reset! v-atom v)))
        b (bucket/mem-cache underlying)]

    ;; test get cache
    (is (= 1 (bucket/get b :k)))
    (is (= {:get 1} @op-counts))
    (is (= 1 (bucket/get b :k)))
    (is (= {:get 1} @op-counts))

    ;; test put cache
    (bucket/put b :k 42)
    (is (= 42 (bucket/get b :k)))
    (is (= 42 @v-atom))
    (is (= {:get 1 :put 1} @op-counts))
    (bucket/put b :k 43)
    (is (= 43 (bucket/get b :k)))
    (is (= 43 @v-atom))
    (is (= {:get 1 :put 2} @op-counts))))

(deftest bounded-write-through-cache-test
  (let [underlying (bucket/->mem-bucket {:a 1 :b 2})
        cache (bucket/bounded-write-through-cache underlying 1)]
    (is-= 1 (bucket/get cache :a))
    (bucket/put underlying :a 42)
    (assert-= 42 (bucket/get underlying :a))
    (testing "cached"
      (is-= 1 (bucket/get cache :a)))
    (testing "cache size"
      (is-= 2 (bucket/get cache :b))
      (is-= 42 (bucket/get cache :a)))
    (testing "write-through"
      (bucket/put cache :a 84)
      (is-= 84 (bucket/get underlying :a)))))

(deftest no-write-through-mem-cache-test
  (let [op-counts (atom {})
        v-atom (atom 1)
        underlying (reify
                     store.bucket.IReadBucket
                     (get [this k] (swap! op-counts update-in [:get] (fnil inc 0))
                       @v-atom)
                     store.bucket.IWriteBucket
                     (put [this k v] (swap! op-counts update-in [:put] (fnil inc 0))
                       (reset! v-atom v)))
        b (bucket/mem-cache underlying (bucket/bucket {}) true)]

    ;; test get cache
    (is (= 1 (bucket/get b :k)))
    (is (= {:get 1} @op-counts))
    (is (= 1 (bucket/get b :k)))
    (is (= {:get 1} @op-counts))

    ;; test put cache
    (bucket/put b :k 42)
    (is (= 42 (bucket/get b :k)))
    (is (= 1 @v-atom))
    (is (= {:get 1} @op-counts))
    (bucket/put b :k 43)
    (is (= 43 (bucket/get b :k)))
    (is (= 1 @v-atom))
    (is (= {:get 1} @op-counts))))

(deftest ->map-test
  (let [b (bucket/bucket {:type :mem})]
    (doto b
      (bucket/put :k "value")
      (bucket/put "x" :y))
    (is-=
     {:k "value" "x" :y}
     (bucket/->map b))))
