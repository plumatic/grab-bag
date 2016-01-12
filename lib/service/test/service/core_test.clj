(ns service.core-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.resource :as resource]
   [crane.config :as config]
   [store.bucket :as bucket]
   [web.fnhouse :as fnhouse]
   [web.server :as server]
   [service.builtin-resources :as builtin-resources]
   [service.core :as service]
   [service.impl :as impl]
   [service.impl-test :as impl-test]
   [service.nameserver :as nameserver]))

(defnk $getter$GET
  {:responses {200 s/Any}}
  [[:resources bucket]]
  {:body (bucket/get bucket "foo")})

(defn build-service-graph [resources]
  (impl/build-service-graph
   resources
   builtin-resources/+builtin-required-resources+
   builtin-resources/+builtin-optional-resources+
   builtin-resources/+builtin-final-resources+))

(deftest test-test
  (let [port (server/get-available-port)
        atom-atom (atom nil)]
    (service/test-service*
     (fn run [s]
       ((:writer s) 100)
       (is (= 100 (bucket/get (:bucket s) "foo")))
       (is (= "100" (slurp (format "http://%s:%s/getter"
                                   (nameserver/lookup-host (:nameserver s) "test-test")
                                   port))))
       (reset! atom-atom (:shutdown-atom s))
       (is (= @@atom-atom :nope)))
     (impl-test/simple-config {:write-key "foo" :init-atom-val :nope})
     (build-service-graph
      [:bucket (graph/instance bucket/bucket-resource {:type :mem})
       :server (graph/instance (fnhouse/simple-fnhouse-server-resource {"" 'service.core-test})
                   {:port port})
       :shutdown-atom (fnk [init-atom-val] (atom init-atom-val))
       :writer (fnk [bucket write-key] (partial bucket/put bucket write-key))
       :done (fnk [shutdown-atom]
               (reify resource/PCloseable (close [this] (reset! shutdown-atom :done))))]))
    (is (= @@atom-atom :done))))

(defnk empty-require [idontcare] (constantly nil))

(deftest bootstrap-resources-test
  (service/test-service*
   (fn run [s]
     (is-= (into #{:send-email :logging-resource :log-data-store :observer :nameserver :snapshot-store :nrepl-server :elb-manager :observer-bundle
                   impl/graph-key :instantiated-resources-atom}
                 (filter keyword? (keys config/Config)))
           (set (keys s)))

     (is (satisfies? plumbing.observer/Observer (:observer s)))
     (is (= ["test-test"] (nameserver/all-service-names (:nameserver s)))))
   (impl-test/simple-config {})
   (build-service-graph []))

  (service/test-service*
   (fn run [s]
     (is-= (into #{:send-email :logging-resource :log-data-store :ta :get-snapshot-store :observer :nameserver :snapshot-store :nrepl-server :elb-manager :observer-bundle
                   impl/graph-key :instantiated-resources-atom}
                 (filter keyword? (keys config/Config)))
           (set (keys s))))
   (impl-test/simple-config {})
   (build-service-graph
    [:ta (graph/instance empty-require [get-snapshot-store] {:idontcare get-snapshot-store})])))
