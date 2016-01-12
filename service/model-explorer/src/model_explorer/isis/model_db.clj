(ns model-explorer.isis.model-db
  "SQL-based store for models"
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.graph :as graph]
   [plumbing.serialize :as serialize]
   [store.bucket :as bucket]
   [store.sql :as sql]
   [model-explorer.core :as model-explorer]
   [model-explorer.isis.db :as db]))

(defnk archived? [[:model-info archived?]]
  archived?)

(defrecord BucketModelStore [b]
  model-explorer/ModelStore
  (all-models [this data-type]
    (->> (bucket/vals b)
         (filter #(= data-type (safe-get % :data-type)))
         (remove archived?)))
  (put-model! [this model]
    (letk [[data-type id] model]
      (bucket/put b [(name data-type) id] model)))
  (model [this data-type id]
    (bucket/get b [(name data-type) id]))
  (archive-model! [this data-type id]
    (bucket/update b [(name data-type) id] (fn-> (assoc-in [:model-info :archived?] true))))
  (unarchive-model! [this data-type id]
    (bucket/update b [(name data-type) id] (fn-> (assoc-in [:model-info :archived?] false)))))

(defnk model-store
  [env {connection-pool (safe-get db/env->+db-spec+ env)} :as args]
  (->BucketModelStore
   (bucket/bucket {})
   #_((graph/instance db/db-bucket []
        {:table "model"
         :primary-key [:data_type :id]
         :->mem (s/fn ^:always-validate to-mem :- model-explorer/Model [d]
                  (-> (map-keys sql/sql->lisp d)
                      (update :data-type keyword)
                      (update :query serialize/deserialize)
                      (update :model-info serialize/deserialize)
                      (update :trainer-params serialize/deserialize)
                      (update :training-data-params serialize/deserialize)))
         :mem-> (s/fn ^:always-validate from-mem [d :- model-explorer/Model]
                  (-> d
                      (update :data-type name)
                      (update :query #(serialize/serialize serialize/+default+ %))
                      (update :model-info #(serialize/serialize serialize/+default+ %))
                      (update :trainer-params #(serialize/serialize serialize/+default+ %))
                      (update :training-data-params #(serialize/serialize serialize/+default+ %))
                      (->> (map-keys sql/lisp->sql))))})
      args)))


(comment
  (require
   '[model-explorer.isis.db :as db]
   '[model-explorer.isis.model-db :as model-db]
   '[clojure.java.jdbc.deprecated :as jdbc]
   '[store.sql :as sql])

  (plumbing.resource/with-open [conn (sql/connection-pool db/+db-spec+)]
    (jdbc/with-connection conn
      (apply sql/create-table-if-missing! "model" model-db/model-table-spec))))
