(ns domain.suggestion-messages
  "Definition of messages pertaining to suggestion activity."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [domain.activity.suggestions :as suggestions]))

(s/defn suggestion-activity-message [interest-id :- Long
                                     action :- suggestions/SuggestionAction]
  {:type :suggestion-activity :id interest-id :action action})
