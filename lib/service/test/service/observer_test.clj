(ns service.observer-test
  (:use clojure.test service.observer plumbing.core)
  (:require
   [plumbing.graph :as graph]
   [plumbing.graph-experimental :as graph-experimental]
   [plumbing.streaming-map :as streaming-map]
   [store.mongo :as mongo]
   [store.snapshots :as snapshots]))

(defmacro with-observer [[obs-sym snap] & body]
  `(with-open [g# (observer-bundle
                   {:snapshot-store ~snap
                    :flush-freq 1
                    :instance {:service-name nil}
                    :log-data-store (mongo/test-log-data-store)})]
     (let [~obs-sym (safe-get g# :the-observer)]
       ~@body)))

(deftest ^{:slow true} observe-graph-test
  (let [snap (snapshots/fresh-test-snapshot-store nil)]
    (with-observer [o snap]
      (let [f (-> (graph/graph
                   :foo (fnk [input] (when (= input :throw) (assert nil)) 1)
                   :bar (fnk [foo] 1))
                  (streaming-map/observer-rewrite o)
                  (graph-experimental/eager-clearing-compile []))]
        (is (thrown? AssertionError (f {:input :throw})))
        (doseq [i [4 2 5]]
          (is (empty? (f {:input i}))))
        (Thread/sleep 1500)
        (let [snaps (snapshots/snapshot-seq snap)]
          (is (>= (count snaps) 1))
          (doseq [s snaps]
            (is (<= 1 (count s) 3))
            (is (contains? s :machine)))
          (is (= 4 (reduce + (remove nil? (map #(get-in % [:foo :count]) snaps)))))
          (is (= 3 (reduce + (remove nil? (map #(get-in % [:bar :count]) snaps))))))))))
