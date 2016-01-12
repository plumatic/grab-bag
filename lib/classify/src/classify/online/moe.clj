(ns classify.online.moe
  "A small client for yelp's MOE library"
  (:use plumbing.core)
  (:require
   [schema.coerce :as coerce]
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.json :as json]
   [plumbing.math :as math]
   [flop.distribution :as distribution]
   [flop.stats :as stats]
   [classify.online :as online]
   [classify.online.policies :as policies]
   [web.client :as client])
  (:import
   [java.util Random]
   [flop.stats UnivariateStats]
   [classify.online Step]))

;; TODO: zero-center, use baseline from A/B test, feed in ratio of revenue - 1

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(s/defschema Point
  "A multi-dimensional point"
  [Double])

(s/defschema ArmStats
  "Schema for historical data on each sampled arm. Each arm is a multi-dimensional Point.
   Note: a common 'gotcha' is to forget to wrap 1-dimensional arms in a vec."
  {Point UnivariateStats})

(s/defschema MoeArms
  {Point (s/protocol distribution/ConjugatePriorDistribution)})

(s/defschema DomainBounds
  {:min Double :max Double})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Exclusively MOE API Schemas

(s/defschema SampledPoint
  "Stats about evaluating an objective function at a point"
  {:point Point
   :value Double
   :value_var Double})

(s/defschema HistoricalInfo
  {:points_sampled [SampledPoint]})

(s/defschema SimpleDomainInfo
  {:dim (s/named s/Int "dimensionality of domain")})

(s/defschema DomainInfo
  (assoc SimpleDomainInfo
    :domain_bounds (s/named [DomainBounds] "a domain bound for each dimension")))

(s/defschema Configuration
  {:domain_info DomainInfo
   (s/optional-key :covariance_info) s/Any
   (s/optional-key :optimizer_info) s/Any})

(s/defschema MeanVarResponse
  {:var [Double]
   :mean [Double]
   :endpoint (s/eq "gp_mean_var_diag")})

(s/defschema NextPointResponse
  {:points_to_sample [Point]
   :status s/Any
   :endpoint (s/eq "gp_next_points_epi")})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helpers

(defn coercer
  "Utility method for constructing a JSON coercer for a shema"
  [s]
  (coerce/coercer s (some-fn coerce/string-coercion-matcher coerce/json-coercion-matcher)))

(def +mean-var-response-coercer+
  (coercer MeanVarResponse))

(def +next-point-response-coercer+
  (coercer NextPointResponse))

(s/defn negate-historical-info :- HistoricalInfo
  "Takes a set of historical points and negates their value (to convert between max and min)"
  [historical-info :- HistoricalInfo]
  (update historical-info :points_sampled (fn->> (map #(update % :value -)))))

(defnk moe-client [{port 6543} {host "10.67.167.154"}]
  (fn [uri body]
    (->> (client/rest-request
          {:port port
           :host host
           :uri uri
           :body body})
         client/safe-body)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; API Methods

(s/defn ^:always-validate mean-variance :- MeanVarResponse
  [client
   configuration :- Configuration
   points-to-evaluate :- [Point]
   historical-info :- HistoricalInfo]
  (->> {:points_to_evaluate points-to-evaluate
        :gp_historical_info historical-info}
       (merge (-> configuration
                  (dissoc :optimizer_info)
                  (update :domain_info select-keys [:dim])))
       (client "/gp/mean_var/diag")
       +mean-var-response-coercer+))

(s/defn ^:always-validate next-point-epi :- NextPointResponse
  [client
   configuration :- Configuration
   num-to-sample :- s/Int
   historical-info :- HistoricalInfo]
  (->> {:num_to_sample num-to-sample
        :gp_historical_info historical-info}
       (merge configuration)
       (client "/gp/next_points/epi")
       +next-point-response-coercer+))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Policy

(s/defn historic-info :- HistoricalInfo
  "Construct historic info from ArmStats"
  [arms :- ArmStats]
  {:points_sampled
   (for [[arm stats] arms
         :let [t (->> stats
                      (distribution/unknown-variance-gaussian 0.0)
                      distribution/posterior-mean-distribution)
               var (distribution/variance t)]
         :when (not (or (Double/isInfinite var) (Double/isNaN var)))]
     (with-meta
       {:value_var var
        :value (distribution/mean t)
        :point arm}
       {:n (safe-get stats :num-obs)}))})

(s/defn domain-info :- DomainInfo
  "Construct a domain info from domain-bounds"
  [domain-bounds :- [DomainBounds]]
  {:dim (count domain-bounds)
   :domain_bounds domain-bounds})

(s/defn moe-arms :- MoeArms
  "Turn historical stats into a set of arms, using MOE to sample a new
   arm and construct pooled means and variances for each arm."
  [client
   configuration :- Configuration
   resolution :- double
   stats :- ArmStats]
  (letk [historic-info (historic-info stats)
         all-points (->> (negate-historical-info historic-info)
                         (next-point-epi client configuration 1)
                         :points_to_sample
                         (mapv (fn->> (mapv (fn-> (/ resolution) Math/round (* resolution)))))
                         (concat (keys stats))
                         distinct)
         [mean var] (mean-variance client configuration all-points historic-info)]
    (zipmap all-points
            (map (fn [m v]
                   (distribution/fixed-posterior
                    (distribution/gaussian m (Math/sqrt v))))
                 mean var))))

;; TODO: add step tracking on arm adding, don't take steps.
(s/defrecord MoePolicy
    [random :- Random
     client
     configuration :- Configuration
     point->action
     update-freq :- (s/named Long "how many steps between arm updates")
     resolution :- double
     steps-to-update :- long
     historical-stats :- ArmStats
     arms :- MoeArms]
  online/PPolicy
  (action [this datum]
    (let [arm (distribution/thompson-sample-best random arms)]
      (online/->ActionWithData
       (point->action datum arm)
       arm)))
  (update-policy [this step]
    (let [updated-stats (update historical-stats (.data ^Step step)
                                (fnil stats/add-obs stats/+empty-uni-stats+) (.reward ^Step step))]
      (if (pos? steps-to-update)
        (assoc this
          :steps-to-update (dec steps-to-update)
          :historical-stats updated-stats)
        (assoc this
          :steps-to-update update-freq
          :historical-stats updated-stats
          :arms (moe-arms client configuration resolution updated-stats)))))
  (split! [this] (assoc this :random (policies/fresh-random)))

  policies/PDecayablePolicy
  (decay [this w]
    (update this :historical-stats (fn->> (map-vals (fn-> (stats/scale-stats w)))))))

(defnk moe-policy
  "A bandit-like online policy that uses Moe under the hood to use a
   gaussian-process to capture dependence between arms. Each arm is a
   multi-dimensional point.

   On `action` an arm is selected via Thompson sampling, and converted
   to an action using the point->action function.

   On `update`, the data is incorporated into the historical stats for
   the appropriate arms.  The sampling distribution remains unaffected
   until 'update-freq' steps have passed, at which time MOE is used to
   draw a new arm (selected via the gp/next-points-epi endpoint), and
   generate new smoothed means and variances for all of the arms (via
   the gp/mean-var-diag endpoint), which will be used for Thompson
   sampling for the next 'update-freq' steps.

   Based on the following example:
    http://engineeringblog.yelp.com/2014/10/using-moe-the-metric-optimization-engine-to-optimize-an-ab-testing-experiment-framework.html"
  [{random (plumbing.MersenneTwister. )}
   client
   configuration :- Configuration
   update-freq :- long
   {point->action (fn [datum arm] arm)}
   {historical-stats :- ArmStats {}}
   {resolution 0.00000001}]
  (MoePolicy.
   random client configuration point->action update-freq resolution update-freq historical-stats
   (moe-arms client configuration resolution historical-stats)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Simple test environment

(defn test-objective-fn
  "multi-peaked fn with optimal over [0 1] at 2/3."
  [^double x]
  (* 0.2 (inc x) (Math/cos (* 3 Math/PI x))))

(defn static-test-environment
  [^java.util.Random r ^long n ^double noise]
  (online/sequential-environment
   (for [i (range n)] {:date (* i 1000)})
   identity
   (constantly :real)
   (fn [_ x]
     (+ (test-objective-fn x) (* noise (.nextGaussian r))))))

(defn visualize-policy
  "Produce a JS string that can be pasted into the console http://localhost:6543/gp/plot to
   visualize the current MOE policy.  Supports a scale parameter to scale the values into
   the range [0 1] which the visualizer is hardcoded for."
  ([^MoePolicy policy] (visualize-policy policy 1.0))
  ([^MoePolicy policy ^double scale]
     (format "optimalLearning.pointsSampled = %s; optimalLearning.updateGraphs();"
             (-> (historic-info (.historical-stats policy))
                 (negate-historical-info)
                 (safe-get :points_sampled)
                 (->> (mapv (fn-> (update :point (fn->> (map #(* scale %)))))))
                 json/generate-string))))

(defn test-moe!
  [batch-size epoch-size num-epochs noise]
  (let [e (static-test-environment
           (plumbing.MersenneTwister.)
           (* epoch-size num-epochs)
           noise)]
    (doseq [[i [reward policy-str arms]]
            (indexed
             (online/epoch-stats
              e
              (policies/decaying-policy
               0.99
               (moe-policy
                {:client (moe-client {:host "192.168.99.100"})
                 :point->action (fn [_ [x]] x)
                 :update-freq batch-size
                 :configuration {:domain_info (domain-info [{:min 0.0 :max 1.0}])
                                 :covariance_info {:covariance_type "square_exponential"
                                                   :hyperparameters [1 0.2]}
                                 :optimizer_info {:optimizer_type "gradient_descent_optimizer"
                                                  :num_multistarts 50
                                                  :num_random_samples 400
                                                  :optimizer_parameters {:max_num_steps 300}}}
                 :resolution 0.01}))
              (fn [steps [_ pol]]
                [(math/mean (map :reward steps))
                 (visualize-policy (:base-policy pol))
                 (map-vals stats/uni-report (:historical-stats (:base-policy pol)))])
              epoch-size))]
      (println "\n" i reward "\n" policy-str #_#_"\n" (pci/pprint-str arms)))))

(set! *warn-on-reflection* false)
