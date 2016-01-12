(ns classify.utils
  (:require
   [hiphip.double :as d]
   [flop.array :as fa]))

(set! *warn-on-reflection* true)

(defn l2-reg-fn
  ([f sigma-sq]
     (let [double-sigma-sq (double (* 2.0 sigma-sq))
           inv-sigma-squared (double (/ 1 sigma-sq))]
       (fn [^doubles weights-arr]
         (let [[v g] (f weights-arr)
               new-v (+ v (d/asum [w weights-arr] (/ (* w w) double-sigma-sq)))]
           (fa/add-in-place! g weights-arr inv-sigma-squared 0.0)
           [new-v g])))))

(defn fancy-l2-reg-fn
  ([f ^doubles prior-vals ^doubles inv-sigma-sq]
     (fn [^doubles weights-arr]
       (let [[v ^doubles g] (f weights-arr)
             offsets-arr ^doubles (fa/interpolate weights-arr 1.0 prior-vals -1.0)]
         [(+ v (d/asum [w offsets-arr iss inv-sigma-sq] (* w w 0.5 iss)))
          (d/afill! [[i v] g offset offsets-arr iss inv-sigma-sq]
                    (+ v (* offset iss)))]))))



(set! *warn-on-reflection* false)
