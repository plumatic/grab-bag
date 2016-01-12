(ns plumbing.graph-experimental-test
  (:use clojure.test plumbing.core plumbing.graph-experimental)
  (:require
   [schema.core :as s]
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.graph :as graph]))

(defn test-graph [u!]
  (graph/graph
   :x (fnk [a b] (u! :x) (str a b))
   :y {:y1 (fnk [a x] (u! :y1) (str a x))
       :y2 (fnk [c y1] (u! :y2) (str c y1))}
   :z (fnk [b [:y y1]] (u! :z) (str b y1))
   :q (fnk [{o 1}] (u! :q) (str o))))

(deftest clearing-map-test
  (is (= {:x #{}
          :y #{:a :c :x}
          :z #{:b :y :z}
          :q #{:o :q}}
         (clearing-map (test-graph (fn [x])) [])))
  (is (= {:x #{}
          :y #{:a :c :x}
          :z #{:b :y}
          :q #{:o}}
         (clearing-map (test-graph (fn [x])) [:z :q]))))

(deftest eager-clearing-compile-test
  (let [rc (atom {})
        u! #(swap! rc update-in [%] (fnil inc 0))]
    (is (= {:x "ab"
            :y {:y1 "aab"
                :y2 "caab"}
            :z "baab"
            :q "1"}
           ((eager-clearing-compile (test-graph u!) [:x :y :z :q])
            {:a "a" :b "b" :c "c"})))
    (is (= @rc (for-map [k [:x :y1 :y2 :z :q]] k 1)))
    (is (= {}
           ((eager-clearing-compile (test-graph u!) [])
            {:a "a" :b "b" :c "c"})))))


(deftest eager-killing-clearing-compile-test
  (let [rc (atom {})
        u! #(swap! rc update-in [%] (fnil inc 0))]
    (is (= {:x "ab"
            :y {:y1 "aab"
                :y2 "caab"}
            :z "baab"
            :q "1"}
           ((eager-killing-clearing-compile (test-graph u!) [:x :y :z :q])
            {:a "a" :b "b" :c "c"})))
    (is (= {:x "ab"
            :y +kill+
            :z +kill+
            :q "1"}
           ((eager-killing-clearing-compile (test-graph u!) [:x :y :z :q])
            {:a "a" :b "b" :c +kill+})))
    (is (= {:x +kill+
            :y +kill+
            :z +kill+
            :q "1"}
           ((eager-killing-clearing-compile (test-graph u!) [:x :y :z :q])
            {:a "a" :b +kill+ :c "c"})))
    (is (= {:x "ab"
            :y {:y1 "aab"
                :y2 "caab"}
            :z "baab"
            :q +kill+}
           ((eager-killing-clearing-compile (test-graph u!) [:x :y :z :q])
            {:a "a" :b "b" :c "c" :o +kill+})))
    (is (= @rc {:x 3 :y1 2 :y2 2 :z 2 :q 3}))))
