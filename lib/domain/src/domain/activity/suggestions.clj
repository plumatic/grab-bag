(ns domain.activity.suggestions
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.index :as index]
   [domain.activity.core :as activity]))

(def +activity-fields+
  "Actions tracked on suggestions."
  [:view
   :follow
   :unfollow
   :remove
   :receive-email ;; Represents how many times we've emailed the suggestion
   ])

(s/defschema SuggestionAction (apply s/enum +activity-fields+))

(s/defn make
  ([]
     (activity/make +activity-fields+))
  ([interest-activity-counts :- {Long activity/ActivityCountsMap}]
     (doto (make)
       (activity/increment-counts! interest-activity-counts))))

(s/defn suggestion-emailed-migrate-iac :- activity/InterestActivityCounts
  [iac :- activity/InterestActivityCounts]
  (if (index/contains (activity/indexer iac) :receive-email)
    iac
    (activity/migrate-iac iac (make))))

(def read-iac
  "Version of actvity/read-iac that includes a migration path for adding an email key"
  (comp suggestion-emailed-migrate-iac activity/read-iac))
