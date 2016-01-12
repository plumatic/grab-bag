(ns service.graft
  "Tools for working with service data, and hotswapping new service definitions."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.resource :as resource]
   [service.impl :as impl]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(s/defschema ServiceGraph
  {(s/named s/Keyword "resource key") (s/named s/Any "Always a fnk definition")})

(s/defschema Service
  {(s/named s/Keyword "resource key") (s/named s/Any "usually the instantiated resource value")
   :instantiated-resources-atom (s/named s/Any "A horrible thing that tracks running services, this should really die")
   impl/graph-key (s/named ServiceGraph "The definition of the graph the created the {resource-key resource-value}")})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helpers

(defn ordered-map
  "Turns a map into an array map ordered by the array of keys."
  [keys map]
  (->> keys
       (mapcat #(if-let [v (get map %)] [% v]))
       (filter identity)
       (apply array-map)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(s/defn find-dependencies-on-resource :- [s/Keyword]
  "Returns a list of resource keys that depend on the resource, including the resource itself at the end."
  [service-graph :- ServiceGraph resource :- s/Keyword]
  (conj (vec (keys (resource/downstream-subgraph service-graph {resource s/Any}))) resource))

(s/defn shutdown-resources!
  "Shuts down selected resources from the current service and returns the remaining resources"
  [current-service :- Service resource-and-dependency-keys :- [s/Keyword]]
  (let [[to-shutdown to-keep] ((juxt filter remove)
                               (comp (set resource-and-dependency-keys) ffirst)
                               @(:instantiated-resources-atom current-service))]
    (resource/shutdown! to-shutdown)
    to-keep))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(defn merge-service-graph
  [old-service-graph new-service-graph]
  (let [new-resources (remove (set (keys old-service-graph)) (keys new-service-graph))
        ;; Here is the definitive ordering of resources
        resource-ordering (concat (keys old-service-graph) new-resources)]
    (ordered-map
     resource-ordering
     (merge old-service-graph
            new-service-graph))))

(defn graft-service-graph
  "Insert the new service sub graph,
  potentially overriding keys in the current service.
  the new service sub graph should be a vector in the
  form of the service description.
  i.e.
  [:foo (fnk [] 42)
   :bar (fnk [foo] 1337)]
  current service can be grabbed from
  user/current-service"
  [current-service new-service-sub-graph]
  (let [service-graph (safe-get current-service :service.impl/graph)
        new-service-sub-graph (graph/->graph (apply array-map new-service-sub-graph))
        resources-to-update (keys new-service-sub-graph)
        resource-and-dependency-key-set
        (set
         (mapcat (partial find-dependencies-on-resource service-graph) resources-to-update))
        ;; Shut down any resources that we are updating or resources the depend on the thing
        ;; we are updating.
        remaining-resources (shutdown-resources! current-service
                                                 (filter resource-and-dependency-key-set (keys service-graph)))
        ;; It's possible that there are new resources that aren't present in the old service graph
        new-resources (remove (set (keys service-graph)) resources-to-update)
        ;; Here is the definitive ordering of resources
        resource-ordering (concat (keys service-graph) new-resources)
        new-service-graph-with-deps (ordered-map
                                     resource-ordering
                                     (merge (select-keys service-graph resource-and-dependency-key-set)
                                            new-service-sub-graph))
        new-service (impl/service
                     (apply dissoc current-service :instantiated-resources-atom impl/graph-key
                            (keys new-service-graph-with-deps))
                     new-service-graph-with-deps)]
    (swap! (:instantiated-resources-atom new-service)
           (fn [xs] (vec (concat remaining-resources xs))))
    (assoc (impl/start new-service)
      impl/graph-key (ordered-map resource-ordering (merge service-graph new-service-graph-with-deps)))))
