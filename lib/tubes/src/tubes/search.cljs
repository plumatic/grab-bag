(ns tubes.search)

(defprotocol SearchProblem
  (successor-states [this state] "return [state transition-score] sequence"))

(defn exhaustive-search
  [search-problem start-state look-ahead]
  (loop [search-nodes [[0.0 [start-state]]]
         iters 0]
    (if (= iters look-ahead)
      ;; Return search-node by score
      (sort-by (comp - first) search-nodes)
      (let [scored-successors (for [[score-so-far states] search-nodes
                                    [succ trans-score] (successor-states search-problem (last states))]
                                [(+ score-so-far trans-score) (conj states succ)])]
        (if (empty? scored-successors)
          (sort-by (comp - first) search-nodes)
          (recur scored-successors (inc iters)))))))
