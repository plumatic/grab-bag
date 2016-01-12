(ns service.impl
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.classpath :as classpath]
   [plumbing.fnk.pfnk :as pfnk]
   [plumbing.fnk.schema :as schema]
   [plumbing.graph :as graph]
   [plumbing.logging :as log]
   [plumbing.resource :as resource]
   [crane.config :as config]
   store.s3))

(set! *warn-on-reflection* true)

(defn project-name
  "Must be called from a deploy ns.  Guesses the project name from *ns*"
  []
  (let [n (subs (#'clojure.core/root-directory (ns-name *ns*)) 1)]
    (assert (not (.contains n "/")))
    (.replace n \_ \-)))

(defn read-config-spec [^String project-name]
  (config/str->config-spec
   (classpath/read-from-classpath
    (str (.replace project-name \- \_) "/config.clj"))))

(defn test-config [abstract-config]
  (config/local-config abstract-config))

(s/defn ^:always-validate build-service-graph
  "Combine, check, and select resource configuration"
  [resources :- (s/both (s/pred vector? 'vector?)
                        (s/pred #(even? (count %)) 'even-count?))
   builtin-required :- {s/Keyword s/Any}
   builtin-optional :- {s/Keyword s/Any}
   builtin-final :- {s/Keyword s/Any}]
  (let [raw-graph (graph/->graph (partition 2 resources))
        optional (resource/upstream-subgraph (vec builtin-optional) (pfnk/input-schema raw-graph))]
    (graph/->graph
     (concat builtin-required
             optional
             (partition 2 resources)
             builtin-final))))

(defn check-service-graph
  "Check that the parameters will satisfy the graph"
  [parameters graph]
  (schema/assert-satisfies-schema
   (pfnk/input-schema graph)
   (assoc (schema/guess-expr-output-schema (test-config parameters))
     :instantiated-resources-atom s/Any)))

;; Note: special instantiated-resources-atom key pointing to atom of instantiated resources, in order.
(def graph-key ::graph)

(defn service [config graph]
  (-> graph
      resource/observer-transform
      (#(resource/resource-transform :instantiated-resources-atom %))
      graph/lazy-compile
      (#(% config))
      (merge config {graph-key graph})))

(defn start [svc]
  (resource/force-walk (safe-get svc graph-key) svc)
  svc)

(defn shutdown [svc]
  (log/warnf "SHUTDOWN SERVICE starting...")
  (resource/shutdown! @(safe-get svc :instantiated-resources-atom))
  (log/warnf "SHUTDOWN SERVICE finished..."))

(set! *warn-on-reflection* false)
