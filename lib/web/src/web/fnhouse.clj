(ns web.fnhouse
  (:use plumbing.core)
  (:require
   [fnhouse.handlers :as fnhouse-handlers]
   [fnhouse.middleware :as middleware]
   [fnhouse.routes :as routes]
   [plumbing.graph :as graph]
   [web.middleware :as web-middleware]
   [web.server :as server]))

(def +default-normal-middleware+
  (comp (partial web-middleware/outermost-middleware web-middleware/req-resp-logger)
        web-middleware/slurp-body-middleware
        web-middleware/simple-default-middleware))

(defn simple-fnhouse-server-resource [handler-ns-map]
  "Simple server from a fnhouse handler map.  Takes server options
   as to server/run-jetty."
  (graph/graph
   :handlers (fnhouse-handlers/nss->handlers-fn handler-ns-map)
   :server (graph/instance server/server-resource
               [env
                handlers
                {join? false}
                {admin? false}
                {normal-middleware +default-normal-middleware+}
                {fnhouse-middleware
                 #(middleware/coercion-middleware % (constantly nil) (constantly nil))}]
             {:join? join?
              :root-handler (->> handlers
                                 (map fnhouse-middleware)
                                 routes/root-handler
                                 ((if (or (not admin?) (= env :local))
                                    identity
                                    web-middleware/admin-middleware))
                                 (web-middleware/format-error-middleware (not= env :prod))
                                 normal-middleware)})))
