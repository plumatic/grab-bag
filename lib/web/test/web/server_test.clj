(ns web.server-test
  (:use clojure.test plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.resource :as resource]
   [web.client :as client]
   [web.fnhouse :as fnhouse]
   [web.server :as server]))


(defnk $upper$GET
  {:responses {200 s/Any}}
  [[:request [:query-params ^String arg]]]
  {:body (.toUpperCase arg)})

(deftest server-test
  (let [port (server/get-available-port)]
    (resource/with-open [server (resource/bundle-run
                                 (fnhouse/simple-fnhouse-server-resource {"" 'web.server-test})
                                 {:env :test :port port})]
      (is (= "UPPER"
             (client/json
              (client/fetch
               :get
               {:host "localhost"
                :port port
                :uri "/upper"
                :query-params {:arg "upper"}})))))))
