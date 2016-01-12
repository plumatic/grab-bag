(ns flop.optimize
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [hiphip.double :as d]
   [plumbing.logging :as log]
   [flop.array :as fa]
   [flop.math :as fm]))

(set! *warn-on-reflection* true)

(def ValueGradientPair [(s/one double "fn value") (s/one doubles "fn gradient")] )
(def DifferentiableFunction (s/=> ValueGradientPair doubles))

(defn remember-last
  "remember last input and output for function f. Assumes
  f only takes one argument, useful for caching recent optimization
  fn queries"
  ([f]
     (let [last (atom nil)]
       (fn [x]
         (assert x)
         (let [[last-arg last-val] @last]
           (if (= last-arg x)
             last-val
             (let [val (f x)]
               (reset! last [x val])
               val))))))
  ([f copy equal?]
     (let [last (atom nil)]
       (fn [x]
         (assert x)
         (let [[last-arg last-val] @last]
           (if (and (not (nil? last-arg)) (equal? last-arg x))
             last-val
             (let [val (f x)]
               (reset! last [(copy x) val])
               val)))))))


(defn affine-fn
  "for number seqs x,y, return f: alpha -> x + alpha* y"
  [^doubles x ^doubles y]
  (fn [alpha] (fa/interpolate x 1.0 y alpha)))

(defn step-fn
  "for a function f, returns the function
   step-size -> f(x0 + step-size * step-dir)

   This is the function which line search would optimzie.

   f: arbitrary function takes seq of numbers as input
   x0: initial point for f
   step-dir: direction for f"
  [f ^doubles x0 ^doubles step-dir]
  (comp f (affine-fn x0 step-dir)))

(defn backtracking-line-search
  "search for step-size in step-dir to satisfy Wolfe Conditions:
     f(x0 + step-size*step-dir) <= f(x0) + suff-descrease * step-size * grad-f(x_0)' delta
   returns satisfying step-size

   f: x -> [f(x) grad-f(x)]
   x0: initial point (use this to get dimension of f). should be seqable
   step-dir: vector of same dimensions as x0. should be seqable

   Options are...
   suff-descrease: Wolfe condition constant (default to 1.0e-4)
   step-size-multiplier: How much to decrease step-size by each iter (default to 0.1)

   TODO: Add curvature lower-bound"
  ([f x0 step-dir
    {:as opts
     :keys [suff-decrease, init-step-size, step-size-multiplier, step-size-underflow]
     :or {suff-decrease 0.001 init-step-size 1.0 step-size-multiplier 0.5 step-size-underflow 1e-20}}]
     (let [[val grad]  (f x0)
           step-fn (step-fn f x0 step-dir)
           directional-deriv (d/dot-product step-dir grad)
           target-val (fn [step-size] (+ val (* suff-decrease step-size directional-deriv)))]
       (loop [step-size init-step-size]
         (when (< step-size step-size-underflow)
           (throw (RuntimeException. (str "Line search: Stepsize underflow. Probably a gradient computation error " (target-val step-size)))))
         (if (<= (-> step-size step-fn first) (target-val step-size))
           step-size
           (recur (* step-size-multiplier step-size)))))))

(defprotocol QuasiNewtonApproximation
  (inv-hessian-times [this ^doubles z] "Implicitly multiply H^-1 z for quasi-newton search direction")
  (update-approx [this ^doubles x-delta ^doubles grad-delta]
    "Update approximation after new x and grad points. Should return a new QuasiNewtonApproximation"))

(deftype LBFGSApproximation [^int max-history-size x-deltas grad-deltas gamma]

  QuasiNewtonApproximation
                                        ; See http://en.wikipedia.org/wiki/L-BFGS
  (inv-hessian-times
    [this z]
    (let [result (d/aclone z)
          alphas (vec (for [[x-delta grad-delta] (map (fn [a b] [a b]) x-deltas grad-deltas)]
                        (let [curvature (d/dot-product x-delta grad-delta)
                              _ (when (< curvature 0)
                                  (throw (RuntimeException.  (format "Non-positive curvature: %.5f %s %s" curvature (seq x-delta) (seq grad-delta)))))
                              alpha (* (/ (d/dot-product x-delta result) curvature))]
                          (fa/add-in-place! result grad-delta (- alpha) 0.0)
                          alpha)))]
      (fa/scale-in-place! result gamma)
      (doseq [[alpha x-delta grad-delta] (reverse (map (fn [a b c] [a b c]) alphas x-deltas grad-deltas))]
        (let [rho (/ (d/dot-product grad-delta result)
                     (d/dot-product x-delta grad-delta))]
          (fa/add-in-place! result x-delta (- alpha rho) 0.0)))
      result))

  (update-approx [this x-delta grad-delta]
    (let [curvature (d/dot-product x-delta grad-delta)
          den (d/dot-product grad-delta grad-delta)
          gamma (/ curvature den)]
      (comment (println (str "curvature: " curvature))
               (println (str "grad-delta-norm: " den))
               (println (str "gamma: " gamma)))
      (LBFGSApproximation.
       max-history-size
       (take max-history-size (conj x-deltas x-delta))
       (take max-history-size (conj grad-deltas grad-delta))
       gamma))))

(defn new-lbfgs-approx [max-history-size]
  (LBFGSApproximation. max-history-size '() '() 1.0))

(defn quasi-newton-iter
  "A single iteration of quasi-newton optimization. Takes same options as quasi-newton-optimize.
   Returns new estimate of arg min f

  f: x => [f(x) grad-f(x)]
  x0: current estimate
  qn-approx: Implements QuasiNewtonApproximation
  for efficiency, you want to implicitly multiply inv-hessian approximation
  with target vector using quasi-newton approximation. "
  ([f ^doubles x0 qn-approx & [opts]]
     (let [grad  (second (f x0))
           step-dir (inv-hessian-times qn-approx grad)
           _ (fa/scale-in-place! step-dir -1.0)
           step-size (backtracking-line-search f x0 step-dir opts)]
       (fa/interpolate x0 1.0 step-dir step-size))))

(defn quasi-newton-optimize
  "Does quasi-newton optimization. Pass in QuasiNewtonApproximation
   to do either exact newton optimization or LBFGS. Parameters:

   qn-approx: Implements QuasiNewtonApproximation (should use (new-lbfgs-approx))
   f: function to optimize
   x0: initial point

   Has several options you don't need to worry about
   max-iters: How many iterations before we bail (default 50)
   step-size-multiplier: Step-size-multiplier for inner line search
   init-step-size-multiplier: Step-size multiplier for first iter (default 0.5).
   For many convex functions, the first line search should be more careful to get
   scale of problems. Definitely true for conditional likelihood objectives. "
  ([f ^doubles x0 qn-approx
    {:as opts
     :keys [max-iters, init-step-size,
            step-size-multiplier, print-progress, thresh iter-callback]
     :or {max-iters 1000, print-progress false, thresh 1e-6}}]
     (log/debugf "opts: %s" (pr-str opts))
     (when print-progress
       (log/infof "\nConfig: %s\n%s Parameters\n" (pr-str opts)  (count x0)))
     (loop [iter 0 x x0 qn-approx qn-approx improvements '()]
       (when iter-callback
         (iter-callback {:iter iter :x x :improvements improvements}))
       (let [[val grad] (f x)
             new-x (quasi-newton-iter f x qn-approx opts)
             [new-val new-grad] (f new-x)
             x-delta (fa/interpolate new-x 1.0 x -1.0)
             grad-delta (fa/interpolate new-grad 1.0 grad -1.0)
             curvature (d/dot-product x-delta grad-delta)
             improvements (if (= new-val 0.0) [1 1 1 1 1] (take 5 (conj improvements (/ (- val new-val) new-val))))
             avg-improve (/ (sum improvements) (count improvements))
             converged? (or (fm/within 1e-20 0.0 curvature)
                            (and (>= (count improvements) 5) (< avg-improve thresh)))]
         (when print-progress
           (log/infof "==>> iter %d, value %.5e --> %.5e (avg improve: %.5E)"
                      iter (double val) (double new-val) (double avg-improve)))
         (if (or converged? (= iter max-iters))
           new-x
           (recur (inc iter) new-x (update-approx qn-approx x-delta grad-delta) improvements))))))

(defn lbfgs-optimize
  "Run LBFGS Optimization. Convenience wrapper for quasi-newton-optimize with LBFGS as the QuasiNewton
   approximation. You can pass in any options that quasi-newton-optimize uses, but also there is a
   :max-history-size (default 9) option specific to LBFGS"
  [f x0 & [{:as opts
            :keys [history-size]
            :or {history-size 15}}]]
  (quasi-newton-optimize
   (remember-last f d/aclone (fa/approx-equal?-fn 1e-100))
   x0
   (new-lbfgs-approx history-size)
   opts))

(defn l2-reg-fn
  ([f sigma-sq]
     (let [double-sigma-sq (double (* 2.0 sigma-sq))
           inv-sigma-squared (double (/ 1 sigma-sq))]
       (fn [^doubles weights-arr]
         (let [[v g] (f weights-arr)
               new-v (+ v (d/asum [w weights-arr] (/ (* w w) double-sigma-sq)))]
           (fa/add-in-place! g weights-arr inv-sigma-squared 0.0)
           [new-v g]))))
  ([f sigma-sq ^doubles prior-vals]
     (let [double-sigma-sq (double (* 2.0 sigma-sq))
           inv-sigma-squared (double (/ 1 sigma-sq))]
       (fn [^doubles weights-arr]
         (let [[v g] (f weights-arr)
               weights-arr (fa/interpolate weights-arr 1.0 prior-vals -1.0)
               new-v (+ v (d/asum [w ^doubles weights-arr] (/ (* w w) double-sigma-sq)))]
           (fa/add-in-place! g weights-arr inv-sigma-squared 0.0)
           [new-v g])))))

(set! *warn-on-reflection* false)
