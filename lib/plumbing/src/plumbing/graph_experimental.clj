(ns plumbing.graph-experimental
  (:use plumbing.core)
  (:require
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.graph :as graph]))

;; TODO: default values that use that
;; https://github.com/plumatic/plumbing/issues/6
;; TODO: generate .invokePrim for primitive hinted fns.

;; TODO: version of expose/hide, optional keys, ability to do &/:as, etc.?
;;  -- consider requiring hierarchically namespaced optional args, like :foo/bar/baz?
;; TODO: special ::kill value that terminates rest of computation of deps (must be clear about subgraph properties)
;;   ^:inline ^:lift-optional ^:multi ^:when ^:clear / ^:leaf/^:used
;; TODO: just generate straight-up let using bodies when possible?
;; TODO: way to compile compilation that doesn't hold onto intermediate results

;; TODO: way to conditionally execute subgraph ........


(defn clearing-map [g out-ks]
  (assert (every? g out-ks))
  (second
   (reduce
    (fn [[needed result] [k node]]
      (let [is-keys (pfnk/input-schema-keys node)
            to-clear (set (remove needed (cons k is-keys)))]
        [(into (disj needed k) is-keys)
         (assoc result k to-clear)]))
    [(set out-ks) {}]
    (reverse (graph/->graph g)))))

(declare eager-clearing-compile)
(defn clearing-compile [g out-ks make-map assoc-f]
  (if (fn? g)
    g
    (let [g (graph/->graph g)
          cm (clearing-map g out-ks)]
      (graph/simple-flat-compile
       (graph/->graph
        (for [[k subg] g] ;; keep ordering
          (let [clear (safe-get cm k)]
            [k (if (contains? clear k)
                 (clearing-compile subg #{} make-map assoc-f)
                 (graph/interpreted-eager-compile subg))])))
       true
       make-map
       (fn [m k f]
         (apply dissoc (assoc-f m k f) (safe-get cm k)))))))

(defn eager-clearing-compile
  "An eager compilation that discards intermediate results not in out-ks as soon as
   they are no longer needed.  Providing out-ks=nil yields a graph that can be run
   for side effects and returns an empty map.
   Note that a subgraph will be cleared as one, not by component."
  [g out-ks]
  (clearing-compile
   g
   out-ks
   (fn [m] m)
   (fn [m k f] (assoc m k (graph/restricted-call f m)))))

(def +kill+ ::kill)

(defn eager-killing-clearing-compile
  "An eager compilation that discards intermediate results not in out-ks as soon as
   they are no longer needed, and stops computations downstream from any +kill+ values.
   Note that if a single input to a subgraph is +kill+, the entire subgraph will be
   killed, and subgraphs will similarly be cleared as one, not by component."
  [g out-ks]
  (clearing-compile
   g
   out-ks
   (fn [m] m)
   (fn [m k f]
     (let [im (select-keys m (pfnk/input-schema-keys f))]
       (assoc m k (if (every? #(not= +kill+ (val %)) im) (f im) +kill+))))))


;; tODO: something for singleton graphs maybe?  You could always just pull the compile trick,
;; or we can put back the graph inlining and pull a helper key thing with a method to hide it.


(comment
  (def g {:a (fnk [x] (Thread/sleep 2000) (+ x 5)) :b (fnk [x] (Thread/sleep 3000) (- x 2)) :c (fnk [a b] (* a b))})
  (time (:c ((eager-compile g) {:x 5})))
  (time (:c ((lazy-compile g) {:x 5})))
  (time (:c ((par-compile g) {:x 5})))

  )
