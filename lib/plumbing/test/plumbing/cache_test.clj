(ns plumbing.cache-test
  (:use plumbing.core clojure.test plumbing.cache plumbing.test)
  (:require
   [plumbing.parallel :as parallel]))

(set! *warn-on-reflection* true)

(deftest strong-interner-test
  (let [i (strong-interner :key)
        v1 {:key 1 :val 10}
        v2 {:key 2 :val 20}]
    (is (identical? v1 (i v1)))
    (is (identical? v2 (i v2)))
    (is (identical? v1 (i {:key 1})))))

(deftest ^{:unit true} bounded-memoize-test
  (let [op-count (atom 0)
        c (bounded-memoize 2 #(do (swap! op-count inc) (inc %)))]
    (is (= (c 1) 2))
    (is (= (c 2) 3))
    (is (= @op-count 2))
    (is (= (c 1) 2))
    (is (= (c 2) 3))
    (is (= @op-count 2))
    (is (= (c 3) 4))
    (is (= (c 2) 3))
    (is (= @op-count 3))
    (is (= (c 1) 2))
    (is (= (c 2) 3))
    (is (= @op-count 4))))

(deftest bounded-memoize-nil-test
  (is (thrown? Exception ((bounded-memoize 2 identity) nil))))

(deftest volatile-bounded-memoize-test
  (testing "entries expire"
    (let [state (atom {})
          f (fn [x] (get (swap! state update x conj x) x))
          mem-f (volatile-bounded-memoize 5 100 f)]
      (testing "soon enough"
        (is (= (mem-f 1) '(1)))
        (Thread/sleep 100)
        (is (= (mem-f 1) '(1 1))))
      (testing "but not too soon"
        (is (= (mem-f 5)
               (mem-f 5)
               '(5))))))
  (testing "entries evict"
    (let [state (atom 0)
          f (fn [_] (swap! state inc))
          mem-f (volatile-bounded-memoize 10 50000 f)]
      (is-= (for [i (range 1 11)]
              (mem-f i))
            (range 1 11))
      (is-= (for [i (range 1 11)]
              (mem-f i))
            (range 1 11))
      (is (= (mem-f 50) 11))
      (is (= (mem-f 1) 12))))
  (testing "don't cache nil"
    (let [state (atom nil)
          f (fn [x] @state)
          mem-f (volatile-bounded-memoize 100 100000 f)
          present-result (fn [result input]
                           (reset! state result)
                           (mem-f input))]
      (is (= nil (present-result nil 0)))
      (is (= 5 (present-result 5 0)))
      (is (= 5 (present-result 4 0)))
      (is (= 5 (present-result nil 0)))))
  (testing "handle key functions"
    (let [f (fnk [mem-arg result]
              result)
          mem-f (volatile-bounded-memoize 100 100000 f :mem-arg)]
      (is (= 5 (mem-f {:mem-arg 0 :result 5})))
      (is (= 5 (mem-f {:mem-arg 0 :result 3})))
      (is (= 3 (mem-f {:mem-arg 1 :result 3}))))))

(deftest thread-local-test
  (let [r (atom 0)
        a (thread-local #(atom 1))
        ts (repeatedly 10 #(doto (Thread. ^Runnable (fn [] (swap! @a inc) (swap! r + @@a))) (.start)))]
    (doseq [^Thread t ts] (.join t))
    (is (= @r 20))))

(deftest cache-with-test
  (let [m (java.util.HashMap.)
        computed (atom 0)]
    (parallel/do-work
     10
     #(cache-with m % (do (Thread/sleep 1) (swap! computed inc) (* % 10)))
     (shuffle (apply concat (repeat 10 (range 100)))))
    (is (= @computed 100))
    (is-= (map-vals deref m)
          (for-map [i (range 100)] i (* i 10)))))

(set! *warn-on-reflection* false)
