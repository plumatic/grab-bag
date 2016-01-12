(ns classify.algorithms.regression
  (:require [schema.core :as s]
            [plumbing.math :as math]
            [flop.optimize :as optimize]
            [flop.map :as map]
            [flop.weight-vector :as weight-vector]
            [hiphip.double :as dbl]
            [classify.index :as index]
            [classify.core :as classify])
  (:import [flop LongDoubleFeatureVector]))

(def Datum [(s/one LongDoubleFeatureVector "feat-vec")
            (s/one double "target")
            (s/optional double "weight")])


(s/defn linear-objective :- optimize/ValueGradientPair
  [num-dims :- long
   data :- [Datum]
   input-weights :- doubles]
  (let [grad (double-array num-dims)
        obj-val (math/sum-od
                 (fn [[^LongDoubleFeatureVector fv target weight]]
                   (let [target (double target)
                         weight (double weight)
                         prediction (.dotProduct fv input-weights)
                         diff (- prediction target)]
                     ;; Update gradient
                     (map/do-fv [[idx val] fv]
                                (dbl/ainc grad  idx (* weight diff val)))
                     ;; Return Updated objective
                     (* 0.5 weight diff diff)))
                 data)]
    [obj-val grad]))

(s/defn optimize-linear :- doubles
  [i-data dimension opts]
  (let [sigma-sq (:sigma-sq opts 0.0)
        base-obj-fn #(linear-objective dimension i-data %)]
    (optimize/lbfgs-optimize
     (if (pos? sigma-sq)
       (optimize/l2-reg-fn base-obj-fn sigma-sq)
       base-obj-fn)
     (double-array dimension)
     (merge {:print-progress true :max-iters 1000 :thresh 1e-6} opts))))

(s/defn learn-linear
  [train-data :- [Datum]
   opts :- java.util.Map]
  (let [[train-data p-index] (index/convert-and-index-data train-data opts)
        weights (optimize-linear train-data (count p-index) opts)]
    (weight-vector/->SparseWeightVector (index/unindex-dense weights p-index))))
