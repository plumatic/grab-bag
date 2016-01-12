(ns domain.docs.external-shares
  "Schematized records for shares by external services. Equivalent to url mentions."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [domain.docs.core :as docs-core])
  (:import
   [clojure.lang Keyword]
   [domain.docs.core IShare]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Data types

;; The source of an external share (reference to doc-poller share bucket)
;; TODO: there should be a SourceSet protocol
(s/defrecord SingletonSourceSet
    [^Keyword type ^Object key])

(declare old-rss-share-id)

(s/defrecord RSSShare
    [action :- (s/enum :canonical :poll)
     feed-url :- String
     feed-name :- (s/maybe String)
     shared-url :- (s/named (s/maybe String) "URL of doc from RSS feed")
     image :- (s/maybe String)
     favicon :- (s/maybe String)
     date :- long
     source-set :- (s/maybe SingletonSourceSet)]
  IShare
  (distinct-by-key [this] [(safe-get this :feed-url) (safe-get this :action)])
  (date [this] date))

(defn old-rss-share-id [^RSSShare s]
  (str (vec (concat [(.feed-url s) (.shared-url s)]
                    (when (= (.action s) :canonical) [:canonical])))))

(s/defrecord TwitterShare
    [id :- long
     action :- (s/enum :tweet :retweet)
     user-id :- long
     screen-name :- String
     name :- (s/maybe String) ;; until 3/20/2015, we filled in screen-name here.
     profile-image :- String
     comment :- (s/maybe String)
     summary-score :- double ;; Measure of similarity of comment to doc body (0-1) (0 if no comment).
     date :- long
     source-set :- (s/maybe SingletonSourceSet)

     ;; Fields below added 3/20/2015, will default to -1 / false/ nil for earlier tweets.
     verified? :- boolean
     num-followers :- int

     reply? :- boolean
     coordinates :- (s/maybe (s/pair double "lat" double "long"))
     num-retweets :- int
     num-favorites :- int
     media-type :- (s/maybe Keyword) ;; :video/:photo/:animated_gif for now.
     ]
  IShare
  (distinct-by-key [this] user-id)
  (date [this] date))

(s/defrecord FacebookShare
    [id :- String
     action :- (s/enum :share)
     user-id :- long
     username :- (s/maybe String)
     name :- (s/maybe String)
     comment :- (s/maybe String)
     date :- long
     source-set :- (s/maybe SingletonSourceSet)]
  IShare
  (distinct-by-key [this] user-id)
  (date [this] date))

(s/defrecord ExternalShares
    [twitter :- [TwitterShare]
     facebook :- [FacebookShare]
     rss :- [RSSShare]])

(defn all-shares [^ExternalShares external-shares]
  (apply concat (vals external-shares)))

(s/defn new-external-shares [^ExternalShares old-shares
                             ^ExternalShares received-shares]
  (merge-with docs-core/new-shares old-shares received-shares))

(defn merge-external-shares
  "Merge shares, keeping the first occurence of each by distinct-by-key"
  [^ExternalShares shares1 ^ExternalShares shares2]
  (merge-with docs-core/concat-shares shares1 shares2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Converting to and from records and old on-disk format

(def read-source-set (docs-core/record-reader map->SingletonSourceSet))
(def write-source-set (docs-core/record-writer (map->SingletonSourceSet {})))

(defn safe-parse-long [x]
  (if (string? x)
    (Long/parseLong x)
    (long x)))

(def +default-twitter-profile-image+
  "https://si0.twimg.com/sticky/default_profile_images/default_profile_1_bigger.png")

(def +tweet-info-ks+
  "Extra keys added on 3/20/2015 to capture stats of user/tweet."
  [:verified? :num-followers :reply? :coordinates
   :num-retweets :num-favorites :media-type])

(defn has-tweet-info? [twitter-share]
  (>= (safe-get twitter-share :num-followers) 0))

(defn share-data->TwitterShare [share]
  (letk [[[:sharer type [:id :as sharer-id] username name image favicon]
          id action comment date source-set {tweet-info nil}] share]
    (map->TwitterShare
     (merge
      {:id (safe-parse-long id)
       :action action
       :user-id (safe-parse-long sharer-id)
       :screen-name username
       ;; for legacy cases, sometimes name and profile-image is missing;
       ;; we never use it anyway, so just fake it
       :name (or name username)
       :profile-image (or image +default-twitter-profile-image+)
       :comment (when comment (:text comment))
       :summary-score (if comment (:summary-score comment) 0.0)
       :date date
       :source-set (read-source-set source-set)}
      (if tweet-info
        (pci/safe-select-keys tweet-info +tweet-info-ks+)
        {:verified? false
         :num-followers -1
         :reply? false
         :coordinates nil
         :num-retweets -1
         :num-favorites -1
         :media-type nil})))))

(defn TwitterShare->old-share-data [^TwitterShare share]
  (letk [[id action user-id screen-name name profile-image comment summary-score date
          source-set] share]
    {:id id
     :action action
     :comment (when comment {:text comment :summary-score summary-score})
     :date date
     :source-set (when source-set (write-source-set source-set))
     :sharer {:type :twitter
              :id user-id
              :name name
              :username screen-name
              :image profile-image
              :favicon nil}
     :tweet-info (when (has-tweet-info? share)
                   (pci/safe-select-keys share +tweet-info-ks+))}))

(defn share-data->RSSShare [share]
  (letk [[[:sharer type [:id :as sharer-id] name username image favicon]
          id action comment date source-set] share]
    (let [[feed-url shared-url _] (read-string id)]
      (map->RSSShare
       {:action action
        :feed-url feed-url
        :feed-name name
        :shared-url shared-url
        :image image
        :favicon favicon
        :date date
        :source-set (when source-set (read-source-set source-set))}))))

(defn canonical-rss-interest [rss-shares-data]
  (when-let [rss-share (first (filter #(= (:action %) :canonical) rss-shares-data))]
    (let [{:keys [id name image favicon]} (:sharer rss-share)]
      (assoc-when
       {:type "feed"
        :key id
        :title name
        :img favicon}))))

(defn RSSShare->old-share-data [^RSSShare share]
  (letk [[action feed-url feed-name shared-url image favicon date source-set] share]
    {:id (old-rss-share-id share)
     :action action
     :comment nil
     :date date
     :source-set (when source-set (write-source-set source-set))
     :sharer {:type :rss
              :id feed-url
              :name feed-name
              :username nil
              :image image
              :favicon favicon}}))

(defn share-data->FacebookShare [share]
  ;; TODO: verify on actual data. can't find FB shares in dead docs (6/12/13)
  (letk [[[:sharer type [:id :as sharer-id] name username image favicon]
          id action comment date source-set] share]
    (map->FacebookShare
     {:id id
      :action action
      :user-id (safe-parse-long sharer-id)
      :username username
      :name name
      :comment (:text comment)
      :date date
      :source-set (when source-set (read-source-set source-set))})))

(defn FacebookShare->old-share-data [^FacebookShare share]
  (letk [[id action user-id username comment date source-set name] share]
    {:id id
     :action action
     :comment (when comment {:text comment :summary-score 1.0})
     :date date
     :source-set (when source-set (write-source-set source-set))
     :sharer {:type :facebook
              :id user-id
              :name name
              :username username
              :image nil
              :favicon nil}}))

(set! *warn-on-reflection* false)
