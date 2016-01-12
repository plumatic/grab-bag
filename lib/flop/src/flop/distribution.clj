(ns flop.distribution
  "Representations for distributions.  Should probably look into Breeze or something instead."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.math :as plumbing-math]
   [flop.array :as array]
   [flop.math :as math]
   [flop.stats :as stats])
  (:import
   [java.util Random]
   [flop.stats UnivariateStats]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protocols

(defprotocol Distribution
  (sample [this random] "Sample a value.  For safe results, use plumbing.MersenneTwister."))

(defprotocol ContinuousDistribution
  (mean [this])
  (variance [this]))

(defn std ^double [d] (Math/sqrt (double (variance d))))

(defprotocol ConjugatePriorDistribution
  (posterior-mean-distribution [this] "Return a posterior distribution, or nil if none yet available.")
  (update-stats [this observation])
  (decay [this wt] "Decay the posterior towards the prior, e.g. w=0.75 means 75% of posterior counts"))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Base distributions

(defrecord Point [x]
  Distribution
  (sample [this r] x)

  ContinuousDistribution
  (mean [this] x)
  (variance [this] 0))

(defrecord Multinomial [^objects objs ^doubles probs uni-report]
  Distribution
  (sample [this r]
    (aget objs (array/sample-discrete r probs)))

  ContinuousDistribution
  (mean [this] (safe-get uni-report :mean))
  (variance [this] (safe-get uni-report :var)))

(s/defn multinomial-distribution
  "A distribution over a set of objects.
   Implements mean and variance if the objects are numeric."
  [vs :- {Object Double}]
  (assert (< (- 1.0 (sum second vs)) 1.0e-8) "Distribution must be normalized.")
  (Multinomial.
   (object-array (keys vs))
   (double-array (vals vs))
   (when (every? number? (keys vs))
     (letk [[sum-xs sum-sq-xs num-obs] (stats/weighted-stats vs)]
       (zipmap [:mean :var] (stats/mean-variance sum-xs sum-sq-xs num-obs))))))

(defn uniform-multinomial-distribution [vs]
  (let [w (/ 1.0 (count vs))]
    (multinomial-distribution (map-from-keys (constantly w) vs))))


(def +improper+
  "Distribution equivalent to a symmetric improper prior, for the purpose
   of sampling."
  (uniform-multinomial-distribution
   [Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY]))

(defrecord Gaussian [^double mean ^double sd]
  Distribution
  (sample [this r] (+ mean (* sd (math/sample-gaussian r))))
  ContinuousDistribution
  (mean [this] mean)
  (variance [this] (plumbing-math/square sd)))

(defn gaussian [mean sd]
  (Gaussian. mean sd))

(defrecord StudentT [^double mean ^double scale ^long dof]
  Distribution
  (sample [this r] (+ mean (* scale (math/sample-t r dof))))
  ContinuousDistribution
  (mean [this] mean)
  (variance [this]
    (* (plumbing-math/square scale)
       (cond (> dof 2) (/ dof (- dof 2))
             (> dof 1) Double/POSITIVE_INFINITY
             :else Double/NaN))))

(defrecord Beta [^double alpha ^double beta]
  Distribution
  (sample [this r] (math/sample-beta r alpha beta))
  ContinuousDistribution
  (mean [this] (/ alpha (+ alpha beta)))
  (variance [this]
    (/ (* alpha beta)
       (* (plumbing-math/square (+ alpha beta)) (+ alpha beta 1)))))

(defrecord Uniform [^double l ^double u]
  Distribution
  (sample [this r] (+ l (* (.nextDouble ^Random r) (- u l))))
  ContinuousDistribution
  (mean [this] (/ (+ l u) 2.0))
  (variance [this] (/ (plumbing-math/square (- u l)) 12.0)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Conjugate priors

(defrecord PointPoint [^double x]
  ConjugatePriorDistribution
  (posterior-mean-distribution [this] (Point. x))
  (update-stats [this x] this)
  (decay [this w] this))

(defrecord BetaBernoulli [^double prior-alpha ^double prior-beta ^double n-pos ^double n-neg]
  ConjugatePriorDistribution
  (posterior-mean-distribution [this]
    (Beta. (+ prior-alpha n-pos) (+ prior-beta n-neg)))
  (update-stats [this x]
    (assert (instance? Boolean x))
    (if x
      (BetaBernoulli. prior-alpha prior-beta (inc n-pos) n-neg)
      (BetaBernoulli. prior-alpha prior-beta n-pos (inc n-neg))))
  (decay [this w]
    (BetaBernoulli. prior-alpha prior-beta (* w n-pos) (* w n-neg))))

(defn uninformative-beta-prior
  "A prior distribution suitable for 0-1 bandits."
  []
  (BetaBernoulli. 0.5 0.5 0.0 0.0))


(defrecord KnownVarianceGaussian [^double prior-mean ^double variance uni-stats]
  ConjugatePriorDistribution
  (posterior-mean-distribution [this]
    (if (< (:num-obs uni-stats) 2)
      +improper+
      (letk [[mean var count] (stats/uni-report uni-stats) ;; not 100% clear if we want sample or regular variance.
             mean-variance (/ (+ (/ variance) (/ count var)))]
        (Gaussian.
         (* mean-variance (+ (* mean count (/ var)) (/ prior-mean variance)))
         (Math/sqrt mean-variance)))))
  (update-stats [this x]
    (KnownVarianceGaussian. prior-mean variance (stats/add-obs uni-stats x)))
  (decay [this w]
    (KnownVarianceGaussian. prior-mean variance (stats/scale-stats uni-stats w))))

(defn known-variance-gaussian
  "Prior for a Gaussian with given prior mean and known variance.
   http://www.cs.ubc.ca/~murphyk/Papers/bayesGauss.pdf"
  [prior-mean variance]
  (KnownVarianceGaussian. prior-mean variance stats/+empty-uni-stats+))


;; using notation from http://www.cs.ubc.ca/~murphyk/Papers/bayesGauss.pdf
(defrecord UnknownVarianceGaussian [^double mu0 ^double kappa0 ^double alpha0 ^double beta0 ^UnivariateStats uni-stats]
  ConjugatePriorDistribution
  (posterior-mean-distribution [this]
    (if (< (double (:num-obs uni-stats)) (max 2 (- 3 (* 2 alpha0))))
      +improper+
      (let [n (.num-obs uni-stats)
            xs (.sum-xs uni-stats)
            xs2 (.sum-sq-xs uni-stats)
            [^double mean ^double var] (stats/sample-mean-variance xs xs2 n) ;; not 100% clear if we want sample or regular variance.
            mu (/ (+ (* kappa0 mu0) (* n mean))
                  (+ kappa0 n))
            kappa (+ kappa0 n)
            alpha (+ alpha0 (/ n 2))
            beta (+ beta0
                    (* (/ (- n 1) 2) var) ;; assumes we are using sample variance
                    (/ (* kappa0 n (* (- mean mu0) (- mean mu0)))
                       (* 2 (+ kappa0 n))))]
        (StudentT. mu (Math/sqrt (/ beta (* alpha kappa))) (* alpha 2)))))
  (update-stats [this x]
    (UnknownVarianceGaussian. mu0 kappa0 alpha0 beta0 (stats/add-obs uni-stats x)))
  (decay [this w]
    (UnknownVarianceGaussian. mu0 kappa0 alpha0 beta0 (stats/scale-stats uni-stats w))))

(defn unknown-variance-gaussian
  "Prior for a Gaussian with given prior mean and uniform standard deviation.
   I think these params follow recommendations in
   http://arxiv.org/pdf/1311.1894v1.pdf
   but not positive."
  ([prior-mean]
     (unknown-variance-gaussian prior-mean stats/+empty-uni-stats+))
  ([prior-mean stats]
     (UnknownVarianceGaussian. prior-mean 0.0 -0.5 0.0 stats)))


(s/defrecord FixedPosterior [d :- (s/protocol Distribution)]
  ConjugatePriorDistribution
  (posterior-mean-distribution [this] d)
  (update-stats [this x] (throw (UnsupportedOperationException. "Not supported")))
  (decay [this w] (throw (UnsupportedOperationException. "Not supported"))))

(s/defn fixed-posterior
  "A simple wrapper method for making a conjugate prior distribution where the posterior
   is a given fixed distribution"
  [d :- (s/protocol ContinuousDistribution)]
  (FixedPosterior. d))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Thompson sampling

(def Arm s/Any)

(s/defschema Arms
  {Arm (s/protocol ConjugatePriorDistribution)})

(deftype KV [k ^double v])

(s/defn thompson-sample-best :- Arm
  "Use Thompson sampling to choose from arms according to posterior mean distribution."
  [r :- Random
   arms :- Arms]
  (.k
   ^KV (reduce-kv
        (fn [^KV kv k arm]
          (let [arm-val (sample (posterior-mean-distribution arm) r)]
            (if (>= arm-val (.v kv))
              (KV. k arm-val)
              kv)))
        (KV. nil Double/NEGATIVE_INFINITY)
        arms)))

(s/defn thompson-best-distribution :- {s/Any double}
  "Return posterior probabilities that each arm is best."
  [r :- Random
   n :- long
   arms :- Arms]
  (let [results (frequencies (repeatedly n #(thompson-sample-best r arms)))]
    (map-from-keys
     #(/ (results % 0) (double n))
     (keys arms))))

(comment
  (require '[flop.distribution :as d])
  (doseq [p [(d/known-variance-gaussian 1 2.0) (d/unknown-variance-gaussian 1)]]
    (println "\n\n" p)
    (let [s (int (* (Math/random) Integer/MAX_VALUE))
          r (plumbing.MersenneTwister. s)]
      (loop [p p i 100]
        (when-not (zero? i)
          (let [s (+ 3 (* 2 (flop.math/sample-gaussian r)))]
            (let [d (d/posterior-mean-distribution p)]
              (println (d/mean d) (d/variance d)))
            (recur (d/update-stats p s) (dec i))))))))

(set! *warn-on-reflection* false)
