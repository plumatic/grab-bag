(ns domain.docs.comments
  (:refer-clojure :exclude [get conj children])
  (:use plumbing.core)
  (:require
   potemkin
   [schema.core :as s]
   [plumbing.error :as err]
   [domain.docs.actions :as actions]
   [domain.docs.core :as docs]
   [web.client :as client])
  (:import
   [com.twitter Regex]
   [domain.docs.actions IAction SimpleAction]
   [domain.docs.core IShare]))

(set! *warn-on-reflection* true)

(s/defrecord UserMention [^long user-id ^long start ^long end])
(s/defrecord URLReference [^String url ^long start ^long end])

(s/defrecord TextEntities
    [user-mentions :- [UserMention]
     urls :- [URLReference]])

(s/defrecord Comment
    [id :- long
     user-id :- long
     parent-id :- (s/maybe long)
     date :- long
     text :- String
     text-entities :- TextEntities
     activity :- [SimpleAction]
     client-type :- (s/maybe docs/ActionClientType) ;; nil pre-Nov 2014
     ;; images :- [CommentImage]
     ;; status :- (s/enum :active :edited :deleted)
     ;; flag-map ;; ???? (removed, marked as spam, ...)
     ;; view-count ;;; mutable?
     ]
  IAction
  (id [this] id)
  (action [this] :comment)
  (user-id [this] user-id)
  (date [this] date)
  (client-type [this] client-type)

  IShare
  (distinct-by-key [this] [:comment id]))

(defn remove-action-from-activity
  [comment-actions action-id]
  (filter (fn [^SimpleAction comment-action]
            (not= action-id (actions/id comment-action)))
          comment-actions))

(potemkin/definterface+ IComments
  ;; Maintains a set of Comment-s, indexed along their tree structure.
  ;; (seq comments) returns a topologically ordered sequence of the Comment-s.
  (get ^Comment [this ^Long id])
  (conj [this ^Comment comment])
  ;; add a comment-action
  (update-activity [this ^Long comment-id ^SimpleAction action])
  (delete-activity [this ^Long comment-id ^Long action-id])
  (children [this ^Long id] "Returns a seq of comment children")
  (top-level [this] "Returns a seq of top-level comments")
  (parent [this ^Long id] "Returns a comment, or nil for top-level"))

(s/defn comment-descendants
  "Returns all the descendants of the specified parent comment-id (includes comment-id)"
  [comments :- IComments
   comment-id :- long]
  (cons (get comments comment-id)
        (for [^Comment child (children comments comment-id)
              desc (comment-descendants comments (.id child))]
          desc)))

(potemkin/deftype+ Comments [parent->children ;; :- {(s/maybe Long) [Long]},  nil for no parent
                             comment-map] ;; :- {Long Comment}
  IComments
  (get [this id] (safe-get comment-map id))
  (conj [this comment]
    (letk [[id parent-id] comment]
      ;; we don't already have this comment
      (assert (not (contains? comment-map id)))
      (when parent-id (assert (contains? comment-map parent-id)))
      (Comments.
       (update-in parent->children [parent-id] clojure.core/conj id)
       (assoc comment-map id comment))))
  (update-activity [this comment-id action]
    (assert (contains? comment-map comment-id))
    (Comments.
     parent->children
     (update-in comment-map [comment-id :activity] docs/merge-shares [action])))
  (delete-activity [this ^Long comment-id ^Long action-id]
    (assert (contains? comment-map comment-id))
    (Comments. parent->children (update-in comment-map [comment-id :activity]
                                           remove-action-from-activity action-id)))
  (children [this id]
    (assert (contains? comment-map id))
    (map #(get this %) (clojure.core/get parent->children id)))
  (top-level [this] (map #(get this %) (clojure.core/get parent->children nil)))
  (parent [this id] (when-let [p (:parent-id (get this id))] (get this p)))

  clojure.lang.Seqable
  (seq [this]
    (seq (for [^Comment top (top-level this)
               child (comment-descendants this (.id top))]
           child)))

  clojure.lang.Counted
  (count [this] (count comment-map))

  Object
  (equals [this x]
    (and (instance? Comments x)
         (= (.comment-map ^Comments x)
            comment-map)))
  (hashCode [this]
    (hash comment-map)))

(def +empty-comments+ (Comments. {} {}))

(defn canonicalize-url
  "Take a possibly messy url extracted from user text and make it a proper url string, or
   return nil if this can't be done."
  [^String url]
  (err/?debug "Error cleaning url" (client/clean-url-query-string url)))

(defn extract-urls
  "Extract things that look like URLs from the text.  This is a hard problem for a bunch of
   reasons -- handling non-canonical URLs, separating URLs from punctuation is ambiguous, etc.
   Let Twitter worry about this for us."
  [^String text]
  (let [matcher (.matcher Regex/VALID_URL text)
        g Regex/VALID_URL_GROUP_URL]
    (loop [urls []]
      (if-not (.find matcher)
        urls
        (recur
         (if-let [canonical-url (canonicalize-url (.group matcher g))]
           (clojure.core/conj urls (URLReference. canonical-url (.start matcher g) (.end matcher g)))
           urls))))))

(defn extract-user-mentions
  "Parse out user mentions from text.  A valid mention is a maximal '@handle' segment
   in the text, where handle obeys the given regex, preceded by the beginning of text, or
   whitespace, or any punctuation other than # or @.
   handle->user-id is a function from prospective handle to user-id, or nil for no match.

   handle-regex is injected in because the canonical version currently lives in
   user-data.user-store.schema, which this project cannot depend on."
  [^String handle-regex handle->user-id ^String text]
  (let [matcher (re-matcher (re-pattern (format "(?:^|[[\\s|\\p{Punct}]-[\\#\\@]])(@[%s]+)" handle-regex)) text)]
    ;; we can't use re-groups because it doesn't give us the start and end of the match.
    (loop [mentions []]
      (if-not (.find matcher)
        mentions
        (let [maybe-handle (subs (.group matcher 1) 1) ; kill the @
              id (handle->user-id maybe-handle)]
          (recur (-> mentions
                     (?> id (clojure.core/conj
                             (UserMention. id (.start matcher 1) (.end matcher 1)))))))))))

(defn disjoint? [entity1 entity2]
  (letk [[[:start :as s1] [:end :as e1]] entity1
         [[:start :as s2] [:end :as e2]] entity2]
    (or (<= e1 s2) (<= e2 s1))))

(defn extract-text-entities
  "Extract mentions and urls from text.  Strings that look like mentions inside of URLs
   are ignored, so the text regions are always disjoint."
  [^String handle-regex handle->user-id ^String text]
  (let [urls (extract-urls text)
        mentions (extract-user-mentions handle-regex handle->user-id text)]
    (TextEntities.
     (remove (fn [e] (not-every? #(disjoint? % e) urls)) mentions)
     urls)))

(defn read-text-entities [text-entities-data]
  (TextEntities. ; handle old docs w/o entities
   (mapv map->UserMention (:user-mentions text-entities-data))
   (mapv map->URLReference (:urls text-entities-data))))

(defn write-text-entities [^TextEntities text-entities]
  (map-vals (fn->> (mapv #(into {} %))) text-entities))

(def read-comment
  (let [base-comment (map->Comment {:id 0 :user-id 0 :date 0})
        reader (docs/record-reader map->Comment base-comment)]
    (fn [data]
      (-> (merge {:client-type nil} data)
          (select-keys (keys base-comment)) ;; allow adding new keys.
          (update-in [:activity] (partial map actions/share-data->Action))
          (update-in [:text-entities] read-text-entities)
          reader))))

(def write-comment
  (let [writer (docs/record-writer (map->Comment {:id 0 :user-id 0 :date 0}))]
    (fn [data]
      (-> data
          (update-in [:activity] (partial map actions/Action->old-grabbag-share))
          (update-in [:text-entities] write-text-entities)
          writer))))

(defn read-comments [comments-data]
  (->> comments-data
       (map read-comment)
       (reduce conj +empty-comments+)))

(defn write-comments [^Comments comments]
  (mapv write-comment comments))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private helpers for mentions

(defn in-spans [i spans]
  (some (fn [[a b]]
          (and (<= a i) (< i b))) spans))

(defn filter-starting-spans [^String s spans]
  (if-not (seq s) s
          (if-let [first-non-whitespace
                   (first (drop-while
                           (fn [i] (or
                                    (in-spans i spans)
                                    (Character/isWhitespace (.charAt s i))))
                           (range (count s))))]
            (subs s first-non-whitespace)
            "")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defnk clean-comment-text
  "removes @mentions at the beginning of the comment."
  [[:text-entities user-mentions] text :as comment]
  (assoc comment :text (->> user-mentions
                            (map (fnk [start end] [start end]))
                            (filter-starting-spans text))))

(defn mentions? [comment user-id]
  (->> (safe-get-in comment [:text-entities :user-mentions])
       (some #(= user-id (safe-get % :user-id)))
       boolean))

(set! *warn-on-reflection* false)
