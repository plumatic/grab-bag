(ns service.riemann
  "Resource for sending events to Riemann and Graphite."
  (:use plumbing.core)
  (:require
   [riemann.client :as client]
   [schema.core :as s]
   [plumbing.resource :as resource]
   [crane.config :as config]
   [service.nameserver :as nameserver]))

(s/defschema RiemannEvent
  {:host (s/named String "service name")
   :service (s/named String "metric name")
   (s/optional-key :state) (s/named String "up to 255 bytes")
   (s/optional-key :time) (s/named long "unix seconds")
   (s/optional-key :description) (s/named String "freeform text")
   (s/optional-key :tags) [String]
   (s/optional-key :metric) s/Num
   (s/optional-key :ttl) (s/named double "time to live in seconds")})

(defprotocol RiemannClient
  (send-events! [this es]
    "Send an event and do not wait for a response.  If Riemann is down or cannot
     be found, the method will fail silently.

     Host will be defaulted to service-name, and time to millis,
     These can be overriden, and other fields can be optionally provided, i.e.:
       service: api v2 *method-skeleton*
       state: (str status code)
       description: stack trace
       tag with:
       - \"api-method\"
       - api-v2
       - client-??
       - hot / cold
     metric: milliseconds to compute response

     Extra tags are also added, which cannot be overriden."))

(defprotocol TestRiemannClient
  (latest-events [this]))

(defnk client [env [:service type] [:instance service-name] nameserver]
  (let [extend-and-validate! (fn [e]
                               (->> (update-in
                                     e [:tags] concat
                                     [(str "env-" (name env))
                                      (str "service-" type)])
                                    (merge
                                     {:host service-name
                                      :time (quot (millis) 1000)})
                                    (s/validate RiemannEvent)))]
    (if-not (config/remote? env)
      (let [events (atom nil)]
        (reify
          RiemannClient
          (send-events! [this es]
            (swap! events #(take 10 (concat (reverse (mapv extend-and-validate! es)) %)))
            (atom nil))
          TestRiemannClient
          (latest-events [this] @events)))

      (let [client (client/tcp-client
                    :host (safe-get (nameserver/lookup-address nameserver "graphite-prod") :host))]
        (reify
          RiemannClient
          (send-events! [this es]
            (client/async-send-events client (mapv extend-and-validate! es)))

          resource/PCloseable
          (close [this] (when client (client/close-client client))))))))
