(ns domain.doc-test-utils
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.hash :as hash]
   plumbing.test
   [domain.docs :as docs]
   [domain.docs.actions :as actions]
   [domain.docs.comments :as comments]
   [domain.docs.fetched-page :as fetched-page]
   [domain.docs.fitness-stats :as fitness-stats]
   [domain.docs.core :as docs-core]
   [domain.docs.external-shares :as external-shares]
   [domain.metadata :as metadata])
  (:import
   [domain.docs Doc]
   [domain.docs.actions SimpleAction]
   [domain.docs.comments Comment Comments]
   [domain.docs.external_shares ExternalShares FacebookShare RSSShare TwitterShare]))

(defn ^RSSShare test-rss-share [share-args]
  (assert share-args)
  (let [share-args (if (map? share-args) share-args {:feed-url (str share-args)})]
    (external-shares/map->RSSShare
     (merge
      {:action :poll
       :feed-url (str "feed-url" (hash share-args))
       :feed-name "feed-name"
       :shared-url "shared-url"
       :image "image"
       :favicon "favicon"
       :date 0
       :source-set nil}
      share-args))))

(defn ^TwitterShare test-twitter-share [share-args]
  (let [share-args (if (map? share-args) share-args {:user-id (long share-args)})]
    (assert (:user-id share-args))
    (external-shares/map->TwitterShare
     (merge
      {:id (hash share-args)
       :action :tweet
       :screen-name (str "screen-name" (:user-id share-args))
       :name "foo"
       :profile-image (str "profile-image" (:user-id share-args))
       :comment "foo"
       :summary-score 0.5
       :date 0
       :source-set nil
       :verified? false
       :num-followers 0
       :reply? false
       :coordinates nil
       :num-retweets 0
       :num-favorites 0
       :media-type nil}
      share-args))))

(defn ^FacebookShare test-facebook-share [share-args]
  (let [share-args (if (map? share-args) share-args {:user-id (long share-args)})]
    (assert (:user-id share-args))
    (external-shares/map->FacebookShare
     (merge
      {:id (str (hash share-args))
       :action :share
       :username (str "username" (:user-id share-args))
       :comment "foo"
       :date 0
       :source-set nil}
      share-args))))

(defn ^ExternalShares test-external-shares [shares-args]
  (let [shares-args (or shares-args {})]
    (assert (every? #{:twitter :facebook :rss} (keys shares-args)))
    (external-shares/map->ExternalShares
     (for-map [[k f] {:twitter test-twitter-share
                      :facebook test-facebook-share
                      :rss test-rss-share}]
       k (let [s (mapv f (get shares-args k))]
           (assert (distinct (map #(docs-core/distinct-by-key %) s)))
           s)))))

(defn ^SimpleAction test-simple-action [action-args]
  (let [action-args (if (map? action-args) action-args {:user-id action-args})]
    (assert (:user-id action-args))
    (actions/map->SimpleAction
     (merge {:id (long (hash action-args))
             :action :bookmark
             :date 0
             :username "username"
             :image "image"
             :client-type :iphone}
            action-args))))

(defn test-simple-actions [actions-args]
  (map test-simple-action actions-args))

(defn test-simple-comment-action [action-args]
  (test-simple-action (merge {:action :comment-bookmark}
                             (if (map? action-args) action-args {:user-id action-args}))))

(defn test-simple-comment-actions [action-args]
  (map test-simple-comment-action action-args))

(defn test-text-entities [te-args]
  (-> (if (map? te-args) te-args {:user-mentions te-args})
      (update-in [:user-mentions]
                 (partial mapv (fn [m] (comments/map->UserMention
                                        (if (map? m) m (zipmap [:user-id :start :end] m))))))
      (update-in [:urls]
                 (partial mapv (fn [m] (comments/map->URLReference
                                        (if (map? m) m (zipmap [:url :start :end] m))))))
      comments/map->TextEntities))

(defn ^Comment test-comment [comment-args]
  (let [comment-args (if (map? comment-args) comment-args {:user-id comment-args})]
    (assert (:user-id comment-args))
    (comments/map->Comment
     (-> {:id (long (hash/murmur64 (pr-str comment-args))) ;; ugh, hash collides.
          :parent-id nil
          :date 0
          :text "Hi!"
          :client-type :android}
         (merge comment-args)
         (update-in [:activity] test-simple-comment-actions)
         (update-in [:text-entities] test-text-entities)))))

(defn ^Comments test-comments
  "Takes comments as a tree, where keys are comment-args and vals
   are true (for leaf) or nested test-comments (for non-leaf).
   Levels with no leaves can be abbreviated as vectors."
  ([comments-args]
     (if (instance? Comments comments-args)
       comments-args
       (test-comments comments/+empty-comments+ nil comments-args)))
  ([comments parent-id comments-args]
     (reduce
      (fn [comments [comment-args children]]
        (let [parent (test-comment (assoc (if (map? comment-args)
                                            comment-args
                                            {:user-id comment-args})
                                     :parent-id parent-id))]
          (-> comments
              (comments/conj parent)
              (?> (not (true? children)) (test-comments (safe-get parent :id) children)))))
      comments
      (if (map? comments-args)
        comments-args
        (map-from-keys (constantly true) comments-args)))))

(defn test-topics [topics]
  (for [t topics]
    (if (string? t)
      [t 0.9 10]
      t)))

(defn test-topic-predictions [topics]
  (for [t topics]
    (if (map? t)
      t
      (zipmap [:id :score :confidence] (if (number? t) [t 0.9 10] t)))))

(defn test-tag [m]
  (assert (:tag m))
  (merge (case (:type m)
           (:user :admin) {:user-id 12345 :date 10000 :info nil}
           (:auto) {:posterior 0.9 :date 10000})
         m))

(defn test-tags [m]
  (for-map [[type tags] m]
    type
    (vec
     (for [tag tags]
       (test-tag
        (assoc (if (map? tag)
                 tag
                 {:tag tag})
          :type type))))))

(defn index-if-needed! [doc index-features!]
  (s/validate domain.docs.Doc doc)
  (when index-features!
    (assert (instance? clojure.lang.IFn index-features!))
    (index-features! doc))
  doc)

(s/defn image :- docs/Image
  [url width height]
  {:url url
   :in-div false
   :prob 0.0
   :size {:height height :width width}})

(defn massage-core-doc-args [doc-args & [interest-manager]]
  (assert (not (:shares doc-args)))
  (assert (:id doc-args))
  (merge {:date 0
          :title "a title"
          :text "some text"
          :images (if (contains? doc-args :images)
                    (:images doc-args)
                    [(image "image.png" 1000 1000)])
          :metadata (metadata/metadata {} [])
          :topics (test-topics (if (contains? doc-args :topics) (:topics doc-args) ["A-topic"]))
          :topic-predictions (cond (:topic-predictions doc-args)
                                   (test-topic-predictions (:topic-predictions doc-args))

                                   (:topics doc-args) ;; index topics if given.
                                   (keep #(docs/topic->topic-prediction interest-manager %)
                                         (test-topics (:topics doc-args))))
          :cluster-features {}
          :cluster-id (safe-get doc-args :id)
          :fitness-stats (doto (fitness-stats/fitness-stats)
                           (fitness-stats/increment-view-count! (:view-count doc-args 0)))
          :in-cluster? false}
         (dissoc doc-args :external-shares :activity :comments :topics :topic-predictions :view-count)
         {:external-shares (test-external-shares (:external-shares doc-args))
          :activity (test-simple-actions (:activity doc-args))
          :comments (test-comments (:comments doc-args))
          :tags (test-tags (:tags doc-args))}))

(defn ensure-feed-id [doc-data interest-manager]
  (if interest-manager
    (assoc doc-data
      :feed-ids (docs/pub-data->feed-ids
                 interest-manager
                 (safe-get doc-data :domain)
                 (safe-get doc-data :canonical-rss-interest)))
    (merge {:feed-ids [4242]} doc-data)))

(defn ^Doc test-external-doc [external-doc-args & [index-features interest-manager]]
  (let [id (:id external-doc-args (long (hash external-doc-args)))]
    (letk [[{url (format "http://%s.com" id)}
            {author {}}
            {html-des {:html "a des" :chunk-offsets []}}
            {domain {}}
            {canonical-rss-interest nil} ;; TODO
            {layout nil}
            {products nil}
            {ner {:title-entities [] :text-entities []}}
            & core-args]
           external-doc-args]
      (index-if-needed!
       (docs/make-ExternalDoc
        (ensure-feed-id
         (merge (massage-core-doc-args (assoc core-args :id id) interest-manager)
                {:url url
                 :author (when author
                           (docs/map->Author (merge {:name "an author" :url "http://author.com"}
                                                    author)))
                 :html-des html-des
                 :domain (when domain
                           (docs/map->Domain (merge {:name "a domain" :title "foo"}
                                                    domain)))
                 :canonical-rss-interest canonical-rss-interest
                 :layout layout
                 :products products
                 :ner ner})
         interest-manager))
       index-features))))

(defn ^Doc test-post [post-args & [index-features interest-manager]]
  (letk [[{submitter-id 1} & core-args] post-args]
    (index-if-needed!
     (docs/make-PostDoc
      (merge
       (massage-core-doc-args
        (merge {:id (long (hash core-args))}
               core-args
               {:feed-ids [docs/+grabbag-post-feed-id+]})
        interest-manager)
       {:submitter-id submitter-id}))
     index-features)))

(defn ^Doc test-doc
  "Make a doc, post iff submitter-id present otherwise external."
  [doc-args & [interest-manager]]
  ((if (:submitter-id doc-args) test-post test-external-doc)
   doc-args nil interest-manager))

(s/defn test-fetched-page :- fetched-page/FetchedPage
  [doc :- Doc]
  {:url (docs/url doc)
   :resolved (docs/url doc)
   :fetch-date (millis)
   :html "<!DOCTYPE><html><head></head><body></body></html>"})

(defn core-doc-fields
  "All doc fields which have value, not reference, equality"
  [doc]
  (update-in doc [:ranking-features] (fn [r] (sort-by key r))))

(defn =-docs [& docs]
  (apply = (map core-doc-fields docs)))

(defmacro is-=-doc [doc1 doc2]
  `(plumbing.test/is-=-by core-doc-fields ~doc1 ~doc2))
