(ns service.core
  (:use plumbing.core)
  (:require
   [plumbing.error :as err]
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.logging :as log]
   [viz.graphviz :as graphviz]
   [crane.config :as config]
   [service.builtin-resources :as builtin-resources]
   [service.impl :as impl]
   [service.remote-repl :as remote-repl]
   [aws.ec2 :as ec2]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private helpers

(defn- add-shutdown-hook! [f]
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. #(err/?error "Error in shutdown hook" (f)))))

(defn start-service [config graph]
  (err/init-logger! :info)
  (err/stfu-apache)
  (try
    (let [s (impl/service config graph)]
      (reset! remote-repl/resources {:config config :service s})
      (add-shutdown-hook! #(impl/shutdown s))
      (impl/start s))
    (catch Throwable t
      (log/errorf t "Error starting service")
      (System/exit 1))))

(defn test-service* [run config graph]
  (let [s (impl/service config graph)]
    (try (reset! remote-repl/resources {:config config :service s})
         (run (impl/start s))
         (finally (impl/shutdown s)))))

(defn dependency-graph-data [config]
  (for [[k & done] (next (reductions conj nil (safe-get config :resource-order)))
        :let [done (set (concat done (keys (:parameters config))))
              r    (safe-get (:resources config) k)]
        [other-k req?] (pfnk/input-schema r)
        :when (or req? (contains? done other-k))]
    [other-k k]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(defnk halt-and-catch-fire
  "Returns a fn that kills the machine that runs it"
  [ec2-keys [:instance instance-id]]
  (fn []
    (ec2/stop (ec2/ec2 ec2-keys) [instance-id] false)))

(defn dependency-graph [config]
  (graphviz/graphviz-el
   (dependency-graph-data config)
   (map-from-keys (partial str "PARAM: ") (-> config :parameters keys))))

(defmacro deploy-suck
  "Some crap to temporarily work around crane shortcomings
   (deploy not being in classpath, mandated fn names for config)."
  [service-name]
  (let [real-deploy-ns-sym (symbol (str (name service-name) ".deploy"))]
    `(do (require '~real-deploy-ns-sym)
         (doseq [[sym# var#] (ns-publics (the-ns '~real-deploy-ns-sym))]
           (let [sym# (with-meta sym# (assoc (meta var#) :ns *ns*))]
             (intern *ns* sym# @var#))))))

(defmacro defservice
  "Define a service graph, which will be run with a crane.config/Config read from
   service/config.clj.  Leave the old weird crappy crane interface (a bunch of configs in ns)
   for now.  We expect that crane will make the final config/config call for remote
   services, and call it ourselves for the local versions."
  [resources]
  (let [project-name (impl/project-name)
        config-spec (impl/read-config-spec project-name)]
    `(do
       (def ~'service-graph
         (impl/build-service-graph
          ~resources
          builtin-resources/+builtin-required-resources+
          builtin-resources/+builtin-optional-resources+
          builtin-resources/+builtin-final-resources+))
       ~@(for [config-key (distinct (concat config/+envs+ (keys (:envs config-spec))))]
           `(def ~(if (= :test config-key) 'test-config (symbol (name config-key)))
              (let [abstract-config# (config/abstract-config
                                      (config/enved-config-spec ~config-spec ~config-key)
                                      ~project-name)]
                (impl/check-service-graph abstract-config# ~'service-graph)
                abstract-config#)))
       (def ~'start (fn [c#] (start-service c# ~'service-graph)))

       (defn ~'test-service [run# & [update-config# update-resources#]]
         (test-service*
          run#
          ((or update-config# identity) (impl/test-config ~'test-config))
          ((or update-resources# identity) ~'service-graph))))))
