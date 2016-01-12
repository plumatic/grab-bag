(ns plumbing.core-incubator-test
  (:use clojure.test plumbing.core-incubator plumbing.test))

(deftest maps-test
  (is-= #{1 2 3} (maps inc [0 1 2 0 1 2])))

(deftest assoc-in-when-test
  (is-= (assoc-in-when {:a :b} [:b :c] nil)
        {:a :b})
  (is-= (assoc-in-when {:a :b} [:b :c] :d)
        {:a :b :b {:c :d}})
  (is-= {} (assoc-in-when {} [:a :b] nil))
  (is-= {:a {:b :c}} (assoc-in-when {} [:a :b] :c)))

(deftest partition-evenly-test
  (is (= [[0 1 2] [3 4 5]] (partition-evenly 2 (range 6))))
  (is (= [[0 1 2] [3 4 5 6]] (partition-evenly 2 (range 7))))
  (is (= [[0] [1] [2] [3 4]] (partition-evenly 4 (range 5))))
  (is (= [[0] [1 2] [3 4] [5 6]] (partition-evenly 4 (range 7)))))

(deftest pprint-str-test
  (is-= "{:a [:b \"cccccccccccccccccccccccccccccccccc\"],\n :d \"hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii\"}\n"
        (pprint-str (array-map :a [:b "cccccccccccccccccccccccccccccccccc"] :d "hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii"))))

(deftest shuffle-by-test
  (let [random (java.util.Random. 12345)]
    (is-= [:a :c :d :b]
          (shuffle-by random [:a :b :c :d]))))

(deftest distribute-by-test
  (is-= [1 2 3] (distribute-by (constantly true) [1 2 3]))
  (is-= [1 6 3 4 5 2 7] (distribute-by odd? [6 4 2 1 3 5 7]))
  (is-= ["apple" "orange" "kitten" "amsterdam" "oxen" "axe"]
        (distribute-by first ["apple" "orange" "oxen" "kitten" "amsterdam" "axe"])))

(deftest count-realized-test
  (is-= 3 (count-realized [1 2 3]))
  (is-= 100 (count-realized (doall (map inc (range 100)))))
  (let [s (map inc (range 100))]
    (is-= 0 (count-realized s))
    (doall (take 32 s))
    (is-= 32 (count-realized s))))

(deftest pkeep-test
  (is-= [1 3 5]
        (pkeep
         (fn [x]
           (when (odd? x) x))
         [1 2 3 4 5 6])))

(deftest ensure-keys-test
  (is-= {:a :b :c :d :e :d} (ensure-keys {:a :b} [:c :e] :d))
  (is-= {:a :b :c nil :e nil} (ensure-keys {:a :b} [:c :e]))
  (is-= {:a :b :c :d :e :d} (ensure-keys {:a :b :c nil} [:c :e] :d)))

(deftest safe-select-keys-test
  (is-= {:a :b} (safe-select-keys {:a :b :c :d} [:a]))
  (is (thrown? Throwable (safe-select-keys {:a :b :c :d} [:e]))))

(deftest safe-singleton-test
  (is-= 1 (safe-singleton [1]))
  (is (thrown? Exception (safe-singleton nil)))
  (is (thrown? Exception (safe-singleton [])))
  (is (thrown? Exception (safe-singleton [1 2]))))

(deftest group-map-by-test
  (is-= {:a [1 2] :b [3]}
        (group-map-by first second [[:a 1] [:b 3] [:a 2]])))

(deftest index-by-test
  (is-= {1 {:a 1} 2 {:a 2}}
        (index-by :a [{:a 1} {:a 2}]))
  (is (thrown? Exception (index-by :a [{:a 1} {:a 1}]))))

(deftest map-fsm-test
  (is-= [1 3 6 11]
        (map-fsm (fn [[s i]] [(* s 2) (+ s i)]) 1 (range 4))))

(deftest merge-all-with-test
  (is-= {:a -2}
        (merge-all-with - 0 {:a 2}))
  (is-= {:a -2 :b -5 :c 10}
        (merge-all-with - 0 {:a 1 :c 10} {:a 3 :b 5}))
  (is-= {:a -2 :b -5 :c 10}
        (merge-all-with - 0 {:a 1 :c 10} {:a 3} {:b 5})))

(deftest distinct-by-fast-test
  (is (= [{:id 1 :data "a"}]
         (distinct-by-fast :id
                           [{:id 1 :data "a"}
                            {:id 1 :data "b"}])))
  (is (= [1 2 3 2 1]
         (map second
              (distinct-by-fast
               first
               [[1 1]
                [1 10]
                [17 2]
                [1 12]
                [:foo 3]
                [:foo 3]
                ['bar 2]
                [1 3]
                [3 1]])))))
