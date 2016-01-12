(ns dashboard.deploy
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.graph :as graph]
   [store.sql :as sql]
   [web.fnhouse :as fnhouse]
   [web.handlers :as handlers]
   [service.core :as service]
   [dashboard.data.db-config :as db-config]
   [dashboard.pages.admin :as admin]
   dashboard.pages.admin.api
   [dashboard.pages.home :as home]
   [dashboard.pages.notification :as notification]
   dashboard.pages.notification
   dashboard.pages.retention
   [dashboard.pages.retention :as retention]))


(defnk $GET
  {:responses {200 s/Any}}
  []
  home/home-response)

(defnk $data$GET
  "data for root dashboard"
  {:responses {200 s/Any}}
  [[:resources home]]
  (home/home-data-response home))

(defnk $css$:**$GET
  {:responses {200 s/Any}}
  [[:request [:uri-args **]]]
  (handlers/resource-response #"" (str "dashboard/css/" **)))

(defnk $js$:**$GET
  {:responses {200 s/Any}}
  [[:request [:uri-args **]]]
  (handlers/resource-response #"" (str "dashboard/target/js/" **)))

(service/defservice
  [:db-env (fnk [env]
             (case env
               ;; we shut down :stage, so point at :prod instead
               (:local :stage) :prod
               env))
   :dws-db-spec (fnk [db-env] (sql/connection-pool (safe-get db-config/db-spec db-env)))

   :retention-data retention/retention-data
   :notification-data notification/notification-data
   :home home/home-graph
   :admin admin/admin-resources

   :server (graph/instance (fnhouse/simple-fnhouse-server-resource
                            {"" 'dashboard.deploy
                             "admin" 'dashboard.pages.admin.api
                             "notification" 'dashboard.pages.notification
                             "retention" 'dashboard.pages.retention})
               [[:service server-port]]
             {:port server-port
              :admin? true})])
