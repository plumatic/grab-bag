(ns plumbing.observer-test
  (:use clojure.test)
  (:require [plumbing.observer :as observer]))


(deftest nil-observer-test
  (is (nil? (observer/watch-fn! nil "foo" nil identity)))
  (is (nil? (observer/watch-fn! nil "foo" nil identity nil)))
  (is (nil? (observer/sub-observer nil "foo")))
  (is (empty? (observer/report nil {}))))

(defn counter-to [n]
  (let [a (atom 0)]
    #(if (= @a n) observer/+stop-watching+ (swap! a inc))))

(deftest ^{:slow true} simple-observer-test-watch
  (let [daddy (observer/make-atom-observer 1)
        baby  (observer/sub-observer daddy "foo")
        merge {:merge (fn [o n] (+ n (or o 0)))}]
    (observer/watch-fn! baby "a" merge (counter-to 20))
    (observer/watch-fn! daddy "b" merge (counter-to 10))
    (Thread/sleep 1000)
    (is (=  (observer/report daddy {}) {"b" 55, "foo" {"a" 210}}))))

(deftest ^{:slow true} simple-observer-test-res-watch
  (let [daddy (observer/make-atom-observer 100)
        baby  (observer/sub-observer daddy "foo")
        x     (atom (java.util.ArrayList. [1 2 3 4 5]))]
    (observer/watch-fn! baby "a" {:merge (fn [o n] (+ n (or o 0)))} count @x)
    (Thread/sleep 1000)

    (is (<= 40 (get-in (observer/report daddy {}) ["foo" "a"]) 60))
    (reset! x nil)
    (System/gc)
    (Thread/sleep 200)
    (observer/report daddy {})
    (Thread/sleep 200)

    (is (nil? (get-in (observer/report daddy {}) ["foo" "a"])))))


(deftest ^{:slow true} simple-observer-test-stats
  (let [daddy (observer/make-atom-observer)
        baby  (observer/sub-observer daddy "foo")
        f     (observer/observed-fn
               baby :a :stats
               (fn [a]
                 (case a
                   :sleep (Thread/sleep 100)
                   :throw (throw (RuntimeException.)))))]
    (dotimes [i 5] (f :sleep))
    (is (thrown? Exception (f :throw)))
    (is (thrown? Exception (f :throw)))

    (let [{:keys [p-data-loss avg-time count time] :as r}
          (get-in (observer/report daddy {:duration 500}) ["foo" :a])]
      (is (= (get r "java.lang.RuntimeException") 2))
      (is (= count 7))
      (is (<= 0.25 p-data-loss 0.3))
      (is (<= 50 avg-time 100))
      (is (<= 500 time 700)))))

(deftest simple-observer-test-count
  (let [daddy (observer/make-atom-observer)
        f     (observer/observed-fn
               daddy :a {:type :counts :group vec}
               (constantly nil))]
    (dotimes [i 5] (f :sleep 2))
    (dotimes [i 5] (f :sleep 3))
    (f :throw)
    (f :throw)

    (is (= (observer/report daddy {})
           {:a {:throw 2 :sleep {2 5 3 5}}}))))

(deftest simple-observer-test-counter
  (let [daddy (observer/make-atom-observer)
        c1 (observer/counter daddy :c1)
        c2 (observer/counter (observer/sub-observer daddy :child) :c2)
        c3 (observer/counter daddy :c3)
        ]
    (c1)
    (c1 :foo :bar)
    (c1 :foo :baz)
    (c1 :quux)
    (is (thrown? Exception (c1 :foo)))
    (c2 :bar)
    (c2 :bar)
    (c1 :foo :bar)
    (c1)
    (is (= (observer/report daddy {})
           {:c1 {nil 2 :foo {:bar 2 :baz 1} :quux 1}
            :child {:c2 {:bar 2}}
            :c3 nil}))))

(deftest stats-counter-test
  (let [daddy (observer/make-atom-observer)
        c1 (observer/stats-counter daddy :c1)]
    (is (empty? (:c1 (observer/report daddy {}))))
    (c1 :foo :bar 2)
    (c1 :foo :baz 3)
    (c1 :foo :bar 0)
    (c1 :quux 5)
    (is (= (observer/report daddy {})
           {:c1 {:foo {:bar {:count 2 :mean 1.0 :total 2.0}
                       :baz {:count 1 :mean 3.0 :total 3.0}}
                 :quux {:count 1 :mean 5.0 :total 5.0}}}))))
