(ns tubes.tiling)

(defprotocol PTilingStrategy
  (layout [this items tile-type->dim page-dim]
    "items: sequence of items
     tile-type->dim: map from tile-type id to [width height]
     page-dim: [width height] dimensions of page

     returns sequence of [item tile-type tile-origin]
     which is a subset of items, where tile-type is one of the
     keys of tile-type->dim and tile-origin is the (x,y)
     upper origin coordinate of this tile or returns
     nil if you can't tile with items.

     It should be the case that the return 'covers'
     the page dimensions (aka all tiles in a pattern are
     used by an item)"))

(defn competitive-linking [edges]
  (let [sorted (reverse (sort-by last edges))
        matched (atom {})
        matched-rev (atom {})]
    (doseq [[l r :as edge] sorted]
      (when-not (or (@matched l)
                    (@matched-rev r))
        (swap! matched assoc l edge)
        (swap! matched-rev assoc r edge)))
    (let [best (vals @matched)]
      [(into {} (map (comp vec butlast) best))
       (apply + (map last best))])))

(defn assign-items [pattern items edges]
  (doall
   (map
    (fn [[i j]]
      (let [[tile-origin tile-type] (nth pattern i)
            item (nth items j)]
        [item tile-type tile-origin]))
    edges)))

(defn all-possible-edges
  "Returns a js/Array containing [pattern-idx item-idx score]"
  [items scoring-fn pattern]
  (let [items (apply array items)
        pattern (apply array pattern)
        res (make-array 0)]
    (.forEach
     pattern
     (fn [[_ tile-type] pattern-idx]
       (.forEach
        items
        (fn [item item-idx]
          (.push res (array pattern-idx item-idx
                            (scoring-fn item tile-type)))))))
    res))

(defn ordered-matching-for-pattern
  [items scoring-fn pattern]
  (let [scored-assignment (map
                           (fn [[tile-origin tile-type] item]
                             [(scoring-fn item tile-type)
                              [item tile-type tile-origin]])
                           pattern
                           items)]
    [(->> scored-assignment (map first) (apply +))
     (map last scored-assignment)]))

(defn bipartite-matching-for-pattern
  [items scoring-fn pattern]
  ;; bipartite matching, return [score, assignment]
  ;; assignment is same as PTilingStrategy.layout return
  ;; e.g. [item tile-type tile-origin]

  (let [possible-edges (all-possible-edges items scoring-fn pattern)
        [edges score] (competitive-linking possible-edges)
        assignment (assign-items pattern items edges)]
    [score assignment]))

(defn scored-matching
  "scoring-fn: (item, tile-type) -> score
   get-patterns: (items page-dim, tile-type->dim) sequence
   of patterns representing complete tilings
   e.g. [[x y] tile-type] where [x,y] is tile origin"
  [pattern-matching-fn scoring-fn get-patterns]
  (reify
    PTilingStrategy
    (layout [this items tile-type->dim page-dim]
      (->> (get-patterns items page-dim tile-type->dim)
           (map (partial pattern-matching-fn items scoring-fn))
           (apply max-key first)
           second))))

(defn ordered-matching
  [scoring-fn get-patterns]
  (scored-matching ordered-matching-for-pattern scoring-fn get-patterns))

(defn bipartite-matching
  [scoring-fn get-patterns]
  (scored-matching bipartite-matching-for-pattern scoring-fn get-patterns))
