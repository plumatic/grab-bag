(ns domain.suggestions
  "Schema for Suggestion Group"
  (:use plumbing.core)
  (:require
   [schema.core :as s]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema

(def +suggestion-sources+
  #{:facebook :twitter :curated :local :grabbag :pocket :google})

(s/defschema SuggestionGroupSource
  (apply s/enum +suggestion-sources+))

(s/defschema SuggestionGroupType
  {:source SuggestionGroupSource
   :type (s/enum :activity :topic)})

(s/defschema SuggestionGroup
  {:type SuggestionGroupType
   :title String
   :image (s/maybe String)})
