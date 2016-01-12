(ns domain.activity.core
  "A mapping from long ids to activity counts. Uses a trove map because
   it's slightly smaller, faster. and serializes/compresses better
   than java map.

   Additionally, tracks the approximate time an interest was last viewed
   (to enable later pruning of inactive items). Counts are represented
   as floats, to enable later time-based decay mechanisms, easy stable
   serialization, and compact in-memory representation.  The last view
   date is stored with a granularity of 1 second by encoding as a
   float, and will wrap in 2038.

   NOTE: be careful using these for ranking features -- if values not
   captured *before* the user sees articles in a session, may leak
   action label information about rare topics/publishers.  For
   example, when a training doc is the only one a user has viewed in a
   topic, we can deduce the actions on the document exactly from the
   interest-activity-counts."
  (:use plumbing.core)
  (:require
   [hiphip.float :as flt]
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.index :as index]
   [plumbing.io :as io]
   [domain.interests.manager :as interests-manager]
   [flop.map :as map])
  (:import
   [gnu.trove TLongObjectHashMap]
   [plumbing.index Index]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas

(def InterestActivityCounts
  {:counts TLongObjectHashMap
   :action-indexer Index})

(s/defschema Action s/Keyword)
(s/defschema Actions [(s/one (s/eq :view) "view") Action])

(def ActivityCounts (Class/forName "[F"))
(def ActivityCountsMap {Action double})

(def IACExplain
  {:counts ActivityCountsMap
   :last-view Long})

(def IACExplainMap
  {(s/named Long "interest-id")
   {:counts ActivityCountsMap
    :last-view Long}})
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public constants

(def ^:const +grabbag-action-scores+
  {:submit 2.0
   :share 1.6
   :comment 1.5
   :bookmark 1.0
   :email 0.5
   :save 0.35
   :click 0.2
   :view 0.0
   :remove -2.0
   :post 2.5
   :comment-bookmark 0.1
   :comment-share 0.1
   :comment-email 0.1
   })

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(s/defn indexer [iac :- InterestActivityCounts]
  (safe-get iac :action-indexer))

(s/defn counts :- TLongObjectHashMap
  [iac :- InterestActivityCounts]
  (safe-get iac :counts))

(s/defn get-index
  [iac :- InterestActivityCounts
   action :- Action]
  (index/index! (indexer iac) action))

(s/defn make-counts :- ActivityCounts
  [^Index indexer]
  (flt/amake [_ (long (count indexer))] (float 0.0)))

(s/defn raw-activity-counts :- (s/maybe ActivityCounts)
  [^InterestActivityCounts iac ^long interest-id]
  (.get (counts iac) interest-id))

(s/defn activity-counts-map :- ActivityCountsMap
  [indexer :- Index
   action-stats :- ActivityCounts]
  (dissoc (for-map [i (range (flt/alength action-stats))]
            (index/item indexer i)
            (double (flt/aget action-stats i)))
          ::last-view-date))

(defn encode-float-time []
  (Float/intBitsToFloat (int (quot (millis) 1000))))

(defn ^long decode-float-time
  [^double time]
  (long (* 1000 (Float/floatToIntBits time))))

(s/defn get! :- ActivityCounts
  "Returns the activity counts associated with the given interest,
   and adds empty counts when the interest is not currently present in the IAC."
  [iac :- InterestActivityCounts
   interest-id :- Long]
  (or (raw-activity-counts iac interest-id)
      (let [stats (make-counts (indexer iac))]
        (.put (counts iac) interest-id stats)
        stats)))

(def +old-actions+ [:view
                    :remove
                    :click
                    :save
                    :email
                    :bookmark
                    :share
                    :comment
                    :submit
                    :post])

(s/defn read-old-iac [iac]
  {:counts iac
   :action-indexer (index/static (cons ::last-view-date +old-actions+))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

;; Measured and Trove is slightly smaller and faster, and serializes/compresses a bit better.
(s/defn make :- InterestActivityCounts
  "Construct a new Interest Activity Counts object for the specified seq of actions."
  [actions :- Actions]
  {:counts (map/map->lotrove {})
   :action-indexer (index/static (cons ::last-view-date actions))})

(s/defn activity-counts :- ActivityCountsMap
  "Returns a map from action to count for the specified interest.
   Defaults to zero for all counts when the interest is not present."
  [^InterestActivityCounts iac ^long interest-id]
  (let [indexer (indexer iac)]
    (activity-counts-map indexer
                         (or (raw-activity-counts iac interest-id)
                             (make-counts indexer)))))

(s/defn activity-count :- double
  "Returns the count of the given action for the specified interest.
   Defaults to zero for unseen interests."
  [^InterestActivityCounts iac ^long interest-id ^Action action]
  (if-let [ac (raw-activity-counts iac interest-id)]
    (->> action
         (get-index iac)
         (flt/aget ac)
         double)
    0.0))

(s/defn activity-score :- double
  "Computes the interest-delta score for the given interest using the IACs."
  ([interest-iacs]
     (sum (fn [[action score]] (* score (+grabbag-action-scores+ action))) interest-iacs))
  ([^InterestActivityCounts iac ^long interest-id]
     (activity-score (activity-counts iac interest-id))))

(s/defn normalized-activity-score :- double
  "Normalize the IAC score by view count"
  [iac :- InterestActivityCounts interest :- Long]
  (/ (activity-score iac interest)
     (inc (activity-count iac interest :view))))

(s/defn increment-count! :- Double
  "Increment the specified action by given amount.
   Has the side effect of setting the last viewed time to current time when action is :view.
   Returns the new count."
  [iac :- InterestActivityCounts
   interest-id :- Long
   action :- Action
   amount :- Double]
  (let [action-index (get-index iac action)
        action-stats (get! iac interest-id)]
    (when (= :view action)
      (flt/aset action-stats (get-index iac ::last-view-date) (encode-float-time)))
    (double (flt/ainc action-stats action-index amount))))

(s/defn increment-count-geometric! :- Double
  "Increment the specified action by given amount, and decays the rest
   of the actions geometrically, according to the formulta:

   new_count = decay * count + amount

   Has the side effect of setting the last viewed time to current time
   when action is :view.  Returns the new count."
  [iac :- InterestActivityCounts
   interest-id :- Long
   action :- Action
   amount :- Double
   decay :- Double]
  (let [last-viewed-index (get-index iac ::last-view-date)]
    (when (= :view action)
      (flt/afill! [[i c] (get! iac interest-id)]
                  (if (= i last-viewed-index)
                    c
                    (* decay c)))))
  (increment-count! iac interest-id action amount))


(s/defn last-view-date :- Long
  "Returns the last time the specified interest was viewed, defaulting to 0."
  [^InterestActivityCounts iac
   ^Long interest-id]
  (->> ::last-view-date
       (activity-count iac interest-id)
       decode-float-time))

(s/defn explain :- IACExplainMap
  "Returns a map representation of the action counts and last viewed time."
  [^InterestActivityCounts iac]
  (map-from-keys
   (fn [i]
     {:counts (activity-counts iac i)
      :last-view (last-view-date iac i)})
   (.keys (counts iac))))

(s/defn pruned
  "Pruned version of all counts with strictly less than min-views views
   that have not been viewed since last-view-date. (non-destructive)."
  [iac :- InterestActivityCounts
   last-view-date :- long
   min-views :- long]
  (let [res (TLongObjectHashMap.)
        view-index (int (get-index iac :view))
        last-view-index (int (get-index iac ::last-view-date))
        cts ^TLongObjectHashMap (safe-get iac :counts)]
    (doseq [k (.keys cts)]
      (let [^floats raw (.get cts k)]
        (when (or (> (decode-float-time (aget raw last-view-index)) last-view-date)
                  (> (aget raw view-index) min-views))
          (.put res k raw))))
    (assoc iac :counts res)))

(s/defn pos-counts-only :- {(s/named Long "interest-id")
                            ActivityCountsMap}
  "Takes an explain, returns only the counts, and filters all non-pos counts"
  [explain :- IACExplain]
  (pci/filter-vals pos? (safe-get explain :counts)))

(s/defn select-interests :- InterestActivityCounts
  "Returns a new InterestActivityCounts that have the same counts
   as those present in given IAC subsetted to only include the specified interest-ids."
  [iac :- InterestActivityCounts
   interest-ids :- [Long]]
  (update-in iac [:counts]
             (fn [counts] (map/lo-select-keys counts interest-ids #(aclone ^floats %)))))

(s/defn difference :- InterestActivityCounts
  "Returns a new InterestActivityCounts representing the difference between
   current and old, only iterating over keys in current, and leaving viewed dates from current."
  [current :- InterestActivityCounts
   old :- InterestActivityCounts]
  (letk [[action-indexer ^TLongObjectHashMap counts] current]
    (assert (= (seq action-indexer) (seq (:action-indexer old))))
    (let [max-idx (count action-indexer)
          old-counts ^TLongObjectHashMap (safe-get old :counts)
          ret (TLongObjectHashMap.)]
      (doseq [k (.keys counts)]
        (let [^floats out (flt/aclone (.get counts k))]
          (when-let [sub ^floats (.get old-counts k)]
            (flt/afill! [c out o sub :range [1 max-idx]] (- c o)))
          (.put ret k out)))
      (assoc current :counts ret))))

(s/defn total-counts :- ActivityCountsMap
  "Returns the total counts over a given interest type."
  [iac :- InterestActivityCounts interest-type :- s/Keyword]
  (letk [[action-indexer ^TLongObjectHashMap counts] iac]
    (let [ret (make-counts action-indexer)
          max-idx (count action-indexer)]
      (doseq [k (.keys counts)]
        (when (= interest-type (interests-manager/id-type k))
          (flt/afill! [c ret o (.get counts k) :range [1 max-idx]] (+ c o))))
      (activity-counts-map action-indexer ret))))

(s/defn interest-ids :- longs
  "returns the set of interests containing counts in this IAC"
  [iac :- InterestActivityCounts]
  (.keys (counts iac)))

(s/defn increment-counts! :- InterestActivityCounts
  "A convenience method for incrementing IACs by a map of counts."
  [iac :- InterestActivityCounts
   interest-activity-counts :- {Long ActivityCountsMap}]
  (doseq [[interest-id activity-counts] interest-activity-counts]
    (doseq [[action amount] activity-counts]
      (increment-count! iac interest-id action amount)))
  iac)

(s/defn read-iac :- InterestActivityCounts
  [iac]
  (if (instance? TLongObjectHashMap iac)
    (read-old-iac iac)
    (update-in iac [:action-indexer] io/from-data)))

(s/defn write-iac
  [iac :- InterestActivityCounts]
  (update-in iac [:action-indexer] io/to-data))

(s/defn migrate-activity-counts :- ActivityCounts
  [old-activity-counts :- ActivityCounts
   old-indexer :- Index
   new-indexer :- Index]
  (float-array
   (for [action new-indexer]
     (if (index/contains old-indexer action)
       (flt/aget old-activity-counts
                 (index/index! old-indexer action))
       0))))

(s/defn migrate-iac :- InterestActivityCounts
  "Migrate an old iac into a new iac.
   copies values from old-iac (that exist in new-iac) into new-iac.
   Returns the mutated new-iac"
  [old-iac :- InterestActivityCounts
   new-iac :- InterestActivityCounts]
  (let [old-counts (counts old-iac)
        old-indexer (indexer old-iac)
        new-indexer (indexer new-iac)
        new-counts (counts new-iac)]
    (doseq [k (.keys old-counts)]
      (.put new-counts k
            (migrate-activity-counts
             (.get old-counts k) old-indexer new-indexer)))
    new-iac))


(set! *unchecked-math* false)
(set! *warn-on-reflection* false)
