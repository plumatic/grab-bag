(ns domain.activity.doc-interests
  (:use plumbing.core)
  (:require [domain.activity.core :as activity]
            [schema.core :as s]))

(def +activity-fields+
  "Actions tracked on doc-interests. Cannot be changed without a migration."
  [:view
   :remove
   :click
   :save
   :email
   :bookmark
   :share
   :comment
   :submit
   :post])

(s/defn make
  ([]
     (activity/make +activity-fields+))
  ([interest-activity-counts :- {Long activity/ActivityCountsMap}]
     (doto (make)
       (activity/increment-counts! interest-activity-counts))))
