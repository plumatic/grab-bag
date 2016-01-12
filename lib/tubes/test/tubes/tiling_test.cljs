(ns tubes.tiling-test
  (:use-macros
   [cljs-test.macros :only [deftest is is=]])
  (:require
   [cljs-test.core :as test]
   [tubes.tiling :as tiling]))

(deftest matching
  (let [[a b c] [0 1 2]
        patterns [ [[[0 0] a] [[0 1] c]]
                   [[[0 0] c] [[0 1] b]] ]
        bipartite-matcher (tiling/bipartite-matching
                           (fn [item tile-type]
                             (* item tile-type))
                           (constantly patterns))
        ordered-matcher (tiling/ordered-matching
                         (fn [item tile-type]
                           (* item tile-type))
                         (constantly patterns))]

    ;; In the case of a tie the matcher picks the last tied pattern
    (is= (tiling/layout bipartite-matcher [0 0] nil nil)
         [[0 c [0 0]] [0 b [0 1]]])
    (is= (tiling/layout bipartite-matcher [5 5] nil nil)
         [[5 c [0 0]] [5 b [0 1]]])

    (is= (tiling/layout bipartite-matcher [1 2] nil nil)
         [[2 c [0 0]] [1 b [0 1]]])

    (is= (tiling/layout ordered-matcher [0 0] nil nil)
         [[0 c [0 0]] [0 b [0 1]]])
    (is= (tiling/layout ordered-matcher [1 2] nil nil)
         [[1 c [0 0]] [2 b [0 1]]])))