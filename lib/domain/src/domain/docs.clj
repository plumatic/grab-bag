(ns domain.docs
  "Schematized records for all documents, activity and shares.

   Use make-ExternalDoc and make-Post to create documents. For test fixtures, refer to
   doc-test-utils.

   write-doc converts either kind of document to ordinary Clojure data (for serialization
   purposes), and read-doc does the inverse."
  (:use plumbing.core)
  (:require
   potemkin [schema.core :as s]
   [clojure.string :as str]
   [plumbing.core-incubator :as pci]
   [plumbing.error :as err]
   [plumbing.io :as io]
   [plumbing.resource :as resource]
   [domain.docs.actions :as actions]
   [domain.docs.comments :as comments]
   [domain.docs.core :as docs-core]
   [domain.docs.external-shares :as external-shares]
   [domain.docs.fitness-stats :as fitness-stats]
   [domain.docs.products :as products]
   [domain.docs.tags :as tags]
   [domain.docs.views-by-client :as views-by-client]
   [domain.interests.manager :as interests-manager]
   [domain.metadata :as metadata])
  (:import
   [clojure.lang Keyword]
   [gnu.trove TLongDoubleHashMap]
   [flop LongDoubleFeatureVector]
   [domain.docs.actions IAction]
   [domain.docs.comments Comments]
   [domain.docs.external_shares ExternalShares]
   [domain.docs.fitness_stats FitnessStats]))


(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Records and schemata for doc fields

(s/defrecord Domain
    [name :- String
     title :- String
     image :- (s/maybe String)
     favicon :- (s/maybe String)])

(s/defrecord Author
    [name :- String
     url :- (s/maybe String)])

(s/defschema Image
  {:url String
   :size {:height long
          :width long}
   :in-div Boolean
   (s/optional-key :prob) double
   (s/optional-key :orig-size) {(s/optional-key :height) String
                                (s/optional-key :width) String}})

;; where in the html do we find the text, and how much of it?
(s/defschema HtmlDes
  {:html String
   :chunk-offsets [[(s/one long "offset")
                    (s/one long "num-visible-chars")]]})

(def Topic
  [(s/one String "topic-name")
   (s/one Number "score") ;; posterior for binary-maxent, count for entity, ...
   (s/one Number "confidence")]) ;; count of #preds in model satisfied for binary-maxent, ...

(def FeedId Long)
(def FeedIds [FeedId]) ;; TODO: assert at least one, once old escape-hatch is gone.
(def TopicId Long)

(def TopicPrediction
  {:id TopicId
   :score Number ;; posterior for binary-maxent, count for entity, ...
   :confidence Number})

(def NEREntry
  [(s/one String "entity")
   (s/one String "entity-type")])   ;; TODO : keywordize?

(def NER
  {:title-entities [NEREntry]
   :text-entities [NEREntry]})

(def FeedPortrait
  {:image-key String
   :extension String
   :image-size [(s/one long "width")
                (s/one long "height")]})

(def Layout
  {(s/optional-key :iphone-retina)
   {(s/optional-key :feed-portrait-full) FeedPortrait
    (s/optional-key :feed-portrait-half) FeedPortrait}})

(def ClusterFeatures
  {long double})

(def CanonicalRSSInterest
  {:type (s/enum "feed")
   :key String
   :title String
   (s/optional-key :img) (s/maybe String)
   (s/optional-key :highres-img) (s/maybe String)})

;; only used in index-master, not a core doc key
(def SimClusterFeatures
  {[(s/one Keyword "ner-type") (s/one long "ngram-arity")] TLongDoubleHashMap})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Records and schemata for internal and external Docs

(s/defrecord ExternalInfo
    [url :- String
     author :- (s/maybe Author)
     html-des :- (s/maybe HtmlDes) ;; some really old docs don't have html des.
     domain :- Domain
     canonical-rss-interest :- (s/maybe CanonicalRSSInterest)
     layout :- (s/maybe Layout)
     products :- [products/Mention]
     ner :- (s/maybe NER)])

(s/defrecord PostInfo
    [submitter-id :- long])

(def TypeInfo
  (s/conditional
   #(instance? ExternalInfo %) ExternalInfo
   #(instance? PostInfo %) PostInfo))

(s/defschema Experimental (s/maybe {Keyword s/Any}))

(s/defrecord Doc
    [id :- long
     date :- long
     title :- String
     text :- String
     topics :- [Topic]
     images :- [Image]
     videos :- [Object]
     metadata :- metadata/Metadata
     activity :- [IAction]
     comments :- Comments
     outlinks :- [String]
     external-shares :- ExternalShares
     fitness-stats :- FitnessStats
     ranking-features :- LongDoubleFeatureVector
     ;; used for Wikipedia topic fidelity and for comment text sim in index-master ingest.
     cluster-features :- ClusterFeatures
     in-cluster? :- boolean
     cluster-id :- long
     doc-type :- (s/enum :external :post)
     type-info :- TypeInfo
     feed-ids :- FeedIds
     experimental :- Experimental
     topic-predictions :- [TopicPrediction]
     tags :- tags/Tags
     views-by-client :- views-by-client/ViewsByClient]
  {(s/optional-key :sim-cluster-features) SimClusterFeatures
   ;; this schema should be interest-deltas/InterestDeltaKey, but do to dependency structure
   ;; it's copied here. should get moved up the stack eventually
   (s/optional-key :home-interests) [[(s/one String "type")
                                      (s/one String "key")]]}
  (fnk [doc-type type-info]
    (when-not (instance? (case doc-type
                           :external ExternalInfo
                           :post PostInfo)
                         type-info)
      (throw (IllegalArgumentException. (format "Doc type %s, info type %s"
                                                doc-type (class type-info)))))
    true))


(defn external? [^Doc doc]
  (case (.doc-type doc) :external true :post false))

(defn ^ExternalInfo external-info [^Doc doc]
  (assert (external? doc))
  (.type-info doc))

(defn ^String url [^Doc doc]
  (when (external? doc)
    (.url (external-info doc))))

(defn ^String domain [^Doc doc]
  (when (external? doc)
    (.name ^Domain (.domain (external-info doc)))))

(defn ^PostInfo post-info [^Doc doc]
  (assert (not (external? doc)))
  (.type-info doc))

(def ExternalDoc (s/both Doc (s/pred external?)))
(def PostDoc (s/both Doc (s/pred (complement external?))))

(defn- make-Doc
  "You must call make-PostDoc or make-ExternalDoc now"
  ([doc-map]
     (-> (merge
          {:in-cluster? false
           :cluster-id (long (safe-get doc-map :id))
           :external-shares (external-shares/map->ExternalShares {})
           :comments comments/+empty-comments+
           :tags tags/+empty-tags+
           :metadata (metadata/metadata {} [])
           :ranking-features (LongDoubleFeatureVector.)
           :fitness-stats (fitness-stats/fitness-stats)
           :views-by-client (views-by-client/views-by-client)}
          doc-map)
         (update-in [:id] long)
         (update-in [:date] long)
         map->Doc))
  ([doc-map doc-type type-info]
     (make-Doc
      (assoc doc-map
        :doc-type doc-type
        :type-info type-info))))

(def +external-info-keys+ (keys (map->ExternalInfo {})))

(s/defn ^:always-validate make-ExternalDoc :- ExternalDoc [doc-map]
  (let [[external-map core-map] ((juxt select-keys (partial apply dissoc))
                                 doc-map +external-info-keys+)]
    (make-Doc core-map :external (map->ExternalInfo external-map))))

(def +post-info-keys+ (keys (map->PostInfo {:submitter-id 0})))

(s/defn ^:always-validate make-PostDoc :- PostDoc [doc-map]
  (let [[post-map core-map] ((juxt select-keys (partial apply dissoc))
                             doc-map +post-info-keys+)]
    (make-Doc core-map :post (map->PostInfo post-map))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Accessing and manipulating docs


(defn all-shares-and-activity [doc-or-add-shares-message]
  (concat (external-shares/all-shares (safe-get doc-or-add-shares-message :external-shares))
          (safe-get doc-or-add-shares-message :activity)))

(s/defn all-actions :- [IAction]
  "Includes actions, comments, and a fake post submit action"
  [doc :- Doc]
  (concat
   (.activity doc)
   (.comments doc)
   (when-not (external? doc)
     (let [submitter-id (safe-get (post-info doc) :submitter-id)]
       [(actions/map->SimpleAction
         {:id 12345
          :user-id submitter-id
          :action :submit
          :date (safe-get doc :date)})]))))

(s/defn dwell-times :- [long]
  "Extract a sequence of dwell times (ms) from this document."
  [doc :- Doc]
  (->> doc .activity (keep :dwell-ms)))

(defn Domain->interest [domain]
  (when domain
    (let [{:keys [name title image favicon]} domain]
      (assoc-when
       {:type "feed"
        :key name
        :title title
        :img favicon}
       :highres-img image))))

(def +grabbag-post-feed-id+ 0)
(def +grabbag-post-feed-interest+ ;; also stored in publisher interest bucket
  {:type "feed"
   :key "example.com"
   :img "IMAGE URL"
   :highres-img "IMAGE URL"
   :title "Grabbag Posts"})

(defn ->feed-interest
  "Deprecated full-interest version of canonical-feed-interest-id."
  [^Doc doc]
  (if (external? doc)
    (let [external-doc (external-info doc)]
      (or (.canonical-rss-interest external-doc)
          (Domain->interest (.domain external-doc))))
    +grabbag-post-feed-interest+))

(s/defn canonical-feed-id :- FeedId
  "Get the feed (not interest id) for the canonical publisher, which is the canonical
   RSS feed if present, and otherwise the domain"
  [doc :- Doc]
  (let [feed-id (->> doc
                     (.feed-ids)
                     first)]
    (assert feed-id)
    feed-id))

(s/defn canonical-feed-interest-id
  "Get the interest id for the canonical publisher (see canonical-feed-id)"
  [doc :- Doc]
  (->> doc
       canonical-feed-id
       (interests-manager/interest-id :feed)))

(s/defn all-feed-interest-ids
  "Get the interest ids for all publishers (both rss and domain)"
  [doc :- Doc]
  (->> doc
       (.feed-ids)
       (map (partial interests-manager/interest-id :feed))))

(defn all-topic-interest-ids [^Doc doc]
  (mapv #(interests-manager/interest-id :topic (safe-get % :id))
        (.topic-predictions doc)))

(defn keep-top-topics
  "Filter topics to those with highest confidence, suitable for,
   e.g., displaying on a topic feed."
  [by topics]
  (let [topics (rsort-by by topics)]
    (or (->> topics
             (filter #(> (by %) 0.67))
             (take 7)
             seq)
        (->> topics
             (filter #(> (by %) 0.5))
             (take 2)
             seq))))

(defn top-topic-interest-ids [^Doc doc]
  (mapv #(interests-manager/interest-id :topic (safe-get % :id))
        (keep-top-topics :score (.topic-predictions doc))))

(defn poster-interest-id [^Doc doc]
  (when-not (external? doc)
    (interests-manager/interest-id :grabbag-poster (.submitter-id (post-info doc)))))

(defn feed-keys [^Doc doc]
  (->> (conj-when [(->feed-interest doc)]
                  (when (external? doc)
                    (Domain->interest (.domain (external-info doc)))))
       (map #(safe-get % :key))
       distinct))

(s/defn pub-data->feed-ids :- FeedIds
  [interest-manager domain :- Domain canonical-rss-interest :- (s/maybe CanonicalRSSInterest)]
  (->> [canonical-rss-interest (Domain->interest domain)]
       (remove nil?)
       (map (fn->> (interests-manager/index! interest-manager) interests-manager/id-key-id))
       distinct
       vec))

(s/defn topic->topic-prediction :- (s/maybe TopicPrediction)
  [interest-manager
   [^String topic score confidence] :- Topic]
  (when-not (or (.startsWith topic "ENT ") ;; don't error for old bad staging topics
                (.endsWith topic " ")
                (= topic "LocalContent"))
    (err/?warn (format "Error indexing old topic: %s" (pr-str topic))
               {:id (interests-manager/id-key-id
                     (interests-manager/index! interest-manager {:type :topic :key topic}))
                :score score
                :confidence (or confidence 42)})))

(defn action-count-map
  "Get a human-readable map from action type to count, including comments, simple actions,
   and views"
  [^Doc doc]
  (assoc-when (->> doc :activity (map actions/action) frequencies-fast)
              :view (fitness-stats/view-count (:fitness-stats doc))
              :comment (let [c (count (:comments doc))] (when-not (zero? c) c))))

(defn core-url
  "Extract the core-url for use in distinct-ifying"
  [^String url]
  (let [q-idx (.indexOf url "?")]
    (-> url
        (subs 0 (if (pos? q-idx) q-idx  (count url)))
        (str/replace #"[^\p{Alnum}]" "")
        str/lower-case)))

(defn core-title
  "Extract the core title of the doc for use in distinct-ifying"
  [doc]
  (-> doc
      (safe-get :title)
      (str/replace #"[^\p{Alnum}]" "")
      str/lower-case))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Serialization

(def read-domain (docs-core/record-reader map->Domain))
(def write-domain (docs-core/record-writer (map->Domain {})))

(def read-author (docs-core/record-reader map->Author))
(def write-author (docs-core/record-writer (map->Author {})))

(defn read-and-upgrade-shares [shares]
  {:external-shares (external-shares/map->ExternalShares
                     (for-map [[k f]
                               {:twitter external-shares/share-data->TwitterShare
                                :facebook external-shares/share-data->FacebookShare
                                :rss external-shares/share-data->RSSShare}]
                       k (mapv f (vals (safe-get shares k)))))
   :activity (pci/keepv (partial err/silent actions/share-data->Action) (vals (safe-get shares :grabbag)))})

(defn downgrade-and-write-shares [external-shares activity]
  (let [->old-shares (fn [s] (when (seq s) (map-from-vals :id s)))]
    (assoc
        (for-map [[k f]
                  {:twitter external-shares/TwitterShare->old-share-data
                   :facebook external-shares/FacebookShare->old-share-data
                   :rss external-shares/RSSShare->old-share-data}]
          k (->old-shares (map f (safe-get external-shares k))))
      :grabbag (->old-shares (map actions/Action->old-grabbag-share activity)))))

(defn- ensure-feed-ids
  [doc-data interest-manager]
  (case (:doc-type doc-data)
    :post (assoc doc-data :feed-ids [+grabbag-post-feed-id+])
    :external (if (seq (:feed-ids doc-data))
                doc-data
                (assoc doc-data
                  :feed-ids (pub-data->feed-ids
                             interest-manager
                             (safe-get-in doc-data [:type-info :domain])
                             (safe-get-in doc-data [:type-info :canonical-rss-interest]))))))

(defn ensure-topic-predictions
  [doc-data interest-manager]
  (if (and (seq (safe-get doc-data :topics))
           (empty? (:topic-predictions doc-data)))
    (assoc doc-data :topic-predictions
           (vec (keep #(topic->topic-prediction interest-manager %)
                      (safe-get doc-data :topics))))
    doc-data))

(defn- ensure-id-fields
  "Ensure topic-prediction and feed-ids fields, not populated in some earlier docs,
   are properly filled in (including new :key in topic-predictions)."
  [doc-data interest-manager]
  (-> doc-data
      (ensure-feed-ids interest-manager)
      (ensure-topic-predictions interest-manager)))

(defnk reconcile-fitness-stats-with-views!
  "Force the views in fitness-stats to match the views-by-client when present.
   (To rectify data corruption caused by a bug where the fitness stats were getting
   updated too aggressively.)"
  [views-by-client fitness-stats :as doc]
  (let [view-count (sum val (views-by-client/view-counts views-by-client))]
    (when (pos? view-count)
      (doto fitness-stats
        (fitness-stats/set-view-count! view-count)))
    doc))

(defn- ^Doc read-core-doc [data & [interest-manager]]
  (-> (merge {:feed-ids nil :topic-predictions nil} data)
      (?> interest-manager (ensure-id-fields interest-manager))
      (update-in [:metadata] #(or % (metadata/metadata {} [])))
      (dissoc :shares :sim-cluster-features)
      (merge (read-and-upgrade-shares (:shares data)))
      (update-in [:fitness-stats] #(if % (fitness-stats/read-fitness-stats %)
                                       (fitness-stats/fitness-stats)))
      (update-in [:views-by-client] #(if % (views-by-client/read-views-by-client %)
                                         (views-by-client/views-by-client)))
      reconcile-fitness-stats-with-views!
      (assoc :ranking-features (LongDoubleFeatureVector.))
      (update-in [:comments] comments/read-comments)
      ;; Fix up errors/issues in old docs
      (dissoc :old-ranking-features) ;; present in some old docs.
      (update-in [:outlinks] #(when (seq %) (map str %))) ;; some old docs have URLs in there.
      (update-in [:cluster-features] #(map-keys long %))
      (update-in [:topics] #(for [t %] (if (= (count t) 2) (conj (vec t) 42) t)))
      (update-in [:images]
                 ;;  this abomination is because the code that used to create :images
                 ;;  treated the first image and subsequent ones differently --
                 ;;  the first image was possibly in-div with the field included if so,
                 ;;  and subsequent images were always in-div but the field was always removed.
                 (fn [imgs]
                   (when-let [[f & m] (seq (map (fn-> (update-in [:size :width] long)
                                                      (update-in [:size :height] long))
                                                imgs))]
                     (vec (cons (update-in f [:in-div] boolean)
                                (map #(merge {:in-div true} %) m))))))
      (strict-map->Doc true)))

(def ^:private +core-doc-keys+
  #{:id :date :title :shares :comments :topics :images :videos
    :metadata :text :fitness-stats :ranking-features
    :outlinks :cluster-id :in-cluster? :doc-type :type-info
    :cluster-features :old-ranking-features :sim-cluster-features
    :feed-ids :topic-predictions :experimental :tags :views-by-client})

(s/defn ^:always-validate read-doc :- Doc
  [data & [interest-manager]]
  "Transform doc data from a legacy on-disk format to Doc.  If interest-manager passed, infer
   missing feed-ids and topic-predictions from existing fields.  Otherwise, leave these
fields nil if they are missing."
  (assert (= (:version data) 1))
  (let [[core-doc-data info-data] (-> (merge {:experimental nil :tags tags/+empty-tags+} data)
                                      (dissoc :version :post?)
                                      ((juxt select-keys (partial apply dissoc)) +core-doc-keys+))]
    (read-core-doc
     (merge
      core-doc-data
      (if (:post? data)
        {:doc-type :post
         :type-info (strict-map->PostInfo info-data true)}
        {:doc-type :external
         :type-info (-> info-data
                        (assoc :canonical-rss-interest
                          (external-shares/canonical-rss-interest (vals (safe-get-in data [:shares :rss]))))
                        (update-in [:author] read-author)
                        (update-in [:domain] read-domain)
                        ;; fixes for old docs
                        (update-in [:html-des] (fn [hd]
                                                 (when (:chunk-offsets hd)
                                                   (update-in hd [:chunk-offsets]
                                                              (partial mapv #(mapv long %))))))
                        (update-in [:layout] (fn [m]
                                               (when m
                                                 (-> m
                                                     (select-keys [:iphone-retina])
                                                     (update-in-when
                                                      [:iphone-retina]
                                                      (fn->> (map-vals (fn-> (update-in [:image-size] (fn->> (mapv long)))))))))))
                        (update-in-when [:commerce] (fn [m] (when m (update-in m [:type] keyword))))
                        (update-in [:ner] #(if (empty? %)
                                             {:title-entities [] :text-entities []}
                                             %))
                        products/merge-products-commerce
                        (strict-map->ExternalInfo true))}))
     interest-manager)))

(defn- write-core-doc [new-doc]
  (let [writer (docs-core/record-writer
                (map->Doc {:id 0 :date 0 :in-cluster? false :cluster-id 0}))]
    (-> new-doc
        writer
        (dissoc :doc-type :type-info)
        (update-in [:fitness-stats] fitness-stats/write-fitness-stats)
        (update-in [:views-by-client] views-by-client/write-views-by-client)
        (?> (-> new-doc :metadata vals aconcat empty?) (dissoc :metadata))
        (assoc :shares (downgrade-and-write-shares (safe-get new-doc :external-shares)
                                                   (safe-get new-doc :activity)))
        (dissoc :external-shares :activity :comments)
        ;; dissoc then assoc if non-empty to give us transitional time during redeploy
        ;; (as long as no comments written, docs remain compatible)
        (assoc-when :comments (seq (comments/write-comments (safe-get new-doc :comments))))
        (assoc :ranking-features nil)
        (assoc :sim-cluster-features nil)
        (assoc :version 1))))

(s/defn ^:always-validate write-doc
  "Transform doc into a backwards compatible on-disk format."
  [doc :- Doc]
  (merge (write-core-doc doc)
         (into {} (case (.doc-type doc)
                    :post (assoc (safe-get doc :type-info) :post? true)
                    :external (-> (safe-get doc :type-info)
                                  (dissoc :canonical-rss-interest)
                                  (update-in [:author] write-author)
                                  (update-in [:domain] write-domain))))))

(s/defn clone-writable-fields :- Doc
  "Produce a copy of Doc with all persistable mutable fields copied so that
   changes to doc will not appear in the copy.

   ** Does not copy transient fields such as ranking-features that will be elided
      on calls to write-doc; instead, resets them to default values **."
  [doc :- Doc]
  (-> doc
      (dissoc :sim-cluster-features)
      (assoc :ranking-features (LongDoubleFeatureVector.))
      (update-in [:fitness-stats] fitness-stats/clone)
      (update-in [:views-by-client] views-by-client/clone)))



(extend-type Doc
  io/PDataLiteral
  (to-data [this] [::doc (write-doc this)]))

(extend-type Doc
  resource/HasId
  (identifier [this] (:id this)))

(defmethod io/from-data ::doc [[_ data]]
  (read-doc data))

(set! *warn-on-reflection* false)
