(ns plumbing.queue-test
  (:use plumbing.core clojure.test)
  (:require [plumbing.queue :as queue]))

(defn priority [x y]
  (> (:priority x)
     (:priority y)))

(defn- basic-queue-test [q]
  (queue/offer q "b")
  (queue/offer q "a")
  (is (= 2 (count q)))
  (is (= (queue/peek q) "b"))
  (is (= (queue/poll q) "b")))

(deftest local-queue-test
  (basic-queue-test (queue/local-queue)))

(deftest priority-queue-test
  (let [q (queue/priority-queue)]
    (queue/offer q "b")
    (queue/offer q "a")
    (is (= 2
           (count q)))
    (is (= "a"
           (queue/peek q)))
    (is (= "a"
           (queue/poll q)))
    (is (= "b"
           (queue/poll q)))))

(deftest priority-queue-comparator-test
  (let [q (queue/priority-queue 11 >)]
    (queue/offer-all q [10 300 77 10])
    (is (= 300
           (queue/poll q)))
    (is (= 77
           (queue/poll q)))
    (is (= 10
           (queue/poll q)))
    (is (= 10
           (queue/poll q)))
    (is (nil? (queue/poll q))))

  (let [q (queue/priority-queue 11
                                priority)]
    (queue/offer-all q [{:priority 2
                         :val "medium"}
                        {:priority 1
                         :val "small"}
                        {:priority 3
                         :val "large"}])

    (is (= "large"
           (:val (queue/poll q))))
    (is (= "medium"
           (:val (queue/poll q))))
    (is (= "small"
           (:val (queue/poll q))))))


(deftest process-job-test
  (let [q (doto (queue/priority-queue
                 10 priority)
            (queue/offer-all [{:priority 10
                               :item {:user "alice"}}
                              {:priority 10
                               :item {:user "bob"}}
                              {:priority 10
                               :item {:user "carol"}}]))
        get-job #(:item (queue/poll q))
        f-result (atom [])
        cb-result (atom [])
        f #(swap! f-result conj (str "f-" (:user %)))
        cb #(swap! cb-result conj (str "cb-" (:user %)))]
    ((juxt f cb) (get-job))
    ((juxt f cb) (get-job))
    ((juxt f cb) (get-job))
    (is (= #{"f-alice" "f-bob" "f-carol"}
           (set @f-result)))
    (is (= #{"cb-alice" "cb-bob" "cb-carol"}
           (set @cb-result)))))
