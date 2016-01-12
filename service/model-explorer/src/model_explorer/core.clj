(ns model-explorer.core
  "Core protocol for models."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [model-explorer.query :as query]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(s/defschema Label
  (s/named String "describes a collection of training examples"))

(s/defschema Labels
  {Label {:date Long s/Keyword s/Any}})

(s/defschema ID (s/named String 'id))

(s/defschema Datum s/Any)

(s/defschema LabeledDatum
  {:id ID
   :type s/Keyword
   :created s/Any
   :updated long
   :datum Datum
   :labels (s/maybe Labels)
   :note s/Any
   (s/optional-key :auto-labels) {Label double}
   s/Keyword [(s/named s/Any "related datums")]})

(s/defschema Model
  {:id String
   :data-type (s/named s/Keyword "the type of data to which this model applies")
   :query (s/named query/Query "represents the feature functions for this model")
   :model-info {:archived? Boolean
                s/Keyword s/Any}
   :trainer-params {s/Keyword s/Any}
   :training-data-params
   {:training-labels
    {(s/named Label "name of class (might be new, different from any data label)")
     (s/named [Label] "labels of positive example data for this class")}
    s/Keyword s/Any}})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Protocols

;; Store for a type of training data.
(defprotocol TrainingDataStore
  (id [this] "keyword representing the unique identifier for the family")
  (labels [this] "Get the set of labels")
  (data [this label] "Get the data for a label")
  (datum [this id] "Get the data by id")
  (all-data [this] "Get the full set of data")
  (put! [this data] "Put a set of datums into the store.  May already exist.")
  (label! [this ids new-label label-data] "Add a label to one or more datums")
  (unlabel! [this ids label] "Remove a label from one or more datums"))

(defmulti render-html (fn [type datum] type))

;; Store for models
(defprotocol ModelStore
  (all-models [this data-type] "Get all models for specified type")
  (put-model! [this model] "Save model")
  (model [this data-type id] "Get model by id")
  (archive-model! [this data-type id] "Set this model to be archived")
  (unarchive-model! [this data-type id] "Restore this model from archive"))
