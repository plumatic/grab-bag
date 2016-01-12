(ns plumbing.chm-test
  (:use clojure.test plumbing.chm)
  (:import [java.util.concurrent ConcurrentMap]))


(defn ops-test [^ConcurrentMap m]
  (ensure! m :x (constantly :y))
  (ensure! m :x (constantly :z))
  (inc! m :y 1.0)
  (inc! m :y 2.0)
  (is (= 4.0 (update! m :y (fn [x] (inc x)))))
  (is (= [4.0 -3.0] (update-pair! m :y (fn [x] (- 1 x)))))
  (is (= {:x :y :y -3.0} (into {} m)))

  (is (try-replace! m :foo nil nil))
  (is (not (try-replace! m :foo :v1 :bcf)))
  (is (not (.containsKey m :foo)))
  (is (try-replace! m :foo :v1 nil))
  (is (= (.get m :foo) :v1))
  (is (not (try-replace! m :foo :v2 :v4)))
  (is (not (try-replace! m :foo :v2 nil)))
  (is (= (.get m :foo) :v1))
  (is (try-replace! m :foo :v2 :v1))
  (is (= (.get m :foo) :v2))
  (is (not (try-replace! m :foo nil :v4)))
  (is (not (try-replace! m :foo :v4 :v4)))
  (is (not (try-replace! m :foo nil nil)))
  (is (= (.get m :foo) :v2))
  (is (try-replace! m :foo nil :v2))
  (is (not (.containsKey m :foo))))

(deftest ops-tests
  (ops-test (chm))
  (ops-test (lru-chm 3)))


(deftest lru-test
  (let [m (lru-chm 5)
        hit-count (atom 0)
        miss-count (atom 0)
        gop (fn [x] (if-let [v (.get m x)]
                      (swap! hit-count inc)
                      (do (swap! miss-count inc)
                          (.put m x true))))]
    (dotimes [i 10]
      (gop :foo)
      (gop i))
    (is (= @hit-count 9))
    (is (= @miss-count 11))))
