(ns domain.docs.tags
  "Namespace for 'tags' on docs.  Tags can be manually added by users in rich remove, by admins
   through the tagger interface, or by automated systems (presumably generalizing from admin
   tags)."
  (:refer-clojure :exclude [conj])
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [schema.experimental.abstract-map :as abstract-map]
   [clojure.string :as str]))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas/records

(s/defschema TagType
  "Types of tags.  All are stored on a doc (in Tags) except for :eval-auto,
   which is for evaluation and stored elsewhere (see resource.tags).
    - user is a user tag, from rich remove.
    - admin is a tag added through the admin interfaxce
    - auto is a tag added by an automated system (presumably generalized from admin tags)
    - eval-auto is an evaluation of an auto tag, not actually stored on the doc
      (see resource.tags) to enable quick turn-around on evaluation."
  (s/enum :auto :user :admin :eval-auto))

(def BaseTag
  "An abstract tag"
  {:type TagType
   :tag (s/both String (s/pred (fn [^String s] (= s (.toLowerCase s))) 'lower-case?))
   :date long})

(def +tag-max-length+ 64)
(def +tag-info-max-length+ 256)

(s/defschema BaseUserTagInfo
  (s/both
   String
   (s/pred #(<= (count %) +tag-info-max-length+))))

(def BaseUserTag
  "An abstract tag"
  (merge
   BaseTag
   {:user-id long
    :info (s/maybe BaseUserTagInfo) ;; free text
    }))

(s/defschema UserTag
  "A tag added through rich remove"
  (assoc BaseUserTag
    :type (s/enum :user) ;; make coax happy
    :tag (s/both String ;; offensive, other, malformed, misclassified, low-quality
                 (s/pred #(<= (count %) +tag-max-length+)))))

(s/defschema AdminTag
  "A tag added by an admin."
  (assoc BaseUserTag
    :type (s/enum :admin)))

(s/defschema AutoTag
  "A tag added by an automated system.  The date is the date the model was deployed."
  (assoc BaseTag
    :type (s/enum :auto)
    :posterior Double))

(s/defschema Tag
  (s/conditional (fnk [type] (= type :user)) UserTag
                 (fnk [type] (= type :admin)) AdminTag
                 (fnk [type] (= type :auto)) AutoTag))

(s/defschema Tags
  {(s/optional-key :user) [UserTag]
   (s/optional-key :admin) [AdminTag]
   (s/optional-key :auto) [AutoTag]})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helper fns

(def +empty-tags+ {})

(s/defn conj :- Tags
  [tags :- Tags tag :- Tag]
  (s/validate Tag tag)
  (let [tag-type (safe-get tag :type)
        current-tags (get tags tag-type [])]
    (assoc tags tag-type
           (clojure.core/conj
            (if (= :user tag-type)
              (filterv #(not= (:user-id tag) (:user-id %)) current-tags)
              current-tags)
            tag))))

(s/defn untag :- Tags
  [tags :- Tags tag-type :- (s/enum :admin :auto) tag-value :- String]
  (update tags tag-type #(vec (remove (fnk [tag] (= tag tag-value)) %))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Tag semantics

(s/defn label-tag :- String
  "Build a tag for a positive or negative training example.  Negative tags start with ~."
  [t :- String pos? :- Boolean]
  (if pos? t (str "~" t)))

(s/defn split-tag-label :- (s/pair String "tag" Boolean "label")
  [t :- String]
  "Split a tag into a base tag and positive or negative label (inverse of label-tag)"
  (if (.startsWith t "~")
    [(subs t 1) false]
    [t true]))

(s/defn blocked-tag? :- Boolean
  "Does this admin tag represent a doc that should be blocked from recommendation?"
  [tag-value :- String]
  (.startsWith tag-value "block"))

(s/defn blocked? :- Boolean
  "Does this tags have a blocked admin tag?"
  [tags :- Tags]
  (->> (mapcat tags [:admin :auto])
       (map #(safe-get % :tag))
       (some blocked-tag?)
       boolean))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Tags as attribute hierarchies, like type=article;content=news

(def ^String +kv-split+ "=")
(def ^String +level-split+ ";")

(defn attributed? [^String tag]
  (.contains tag +kv-split+))

(s/defschema Attributes
  "A split representation of an attributed tag.
   e.g., 'type=article;content=news' becomes [['type' 'article'] ['content' 'news']].

   Attributed tags thus form an AND-OR tree of sorts, where every node can have multiple
   sub-attribute keys (e.g., news articles can vary independently according to region and
   whether they are opinion pieces), but values for each key are mutually exclusive
   (e.g., an document can't have 'type' of both 'video' and 'article')."
  [(s/pair String "key" String "value")])

(s/defn ^:always-validate tag->attributes :- Attributes
  [tag :- String]
  (mapv #(vec (.split ^String % +kv-split+)) (.split tag +level-split+)))

(s/defn ^:always-validate attributes->tag :- String
  [attrs :- Attributes]
  (str/join +level-split+ (map #(str/join +kv-split+ %) attrs)))

(s/defn mutex?
  "Returns true iff x is mutually exclusive for y,
   which occurs when, at some level, x and y have different values for
   the same key."
  [tag-x :- String tag-y :- String]
  (loop [[[k1 v1] & xs] (tag->attributes tag-x)
         [[k2 v2] & ys] (tag->attributes tag-y)]
    (cond
     (some nil? [k1 k2]) false
     (not= k1 k2) false
     (not= v1 v2) true
     :else (recur xs ys))))

(s/defn sibling-mutex?
  "returns true iff x and y are siblings in the hierarchy but have different attribute values"
  [tag-x :- String tag-y :- String]
  (and (not= tag-x tag-y) (apply = (map (fn-> tag->attributes aconcat butlast) [tag-x tag-y]))))

(s/defn subclass?
  "Returns true iff y is a subclass of x"
  [tag-x :- String tag-y :- String]
  (.startsWith tag-y tag-x))

(s/defn superclasses :- [String]
  "Returns a set of tags representing the superclasses of the current tag (includes current tag)"
  [tag :- String]
  (->> (tag->attributes tag)
       (iterate drop-last)
       (take-while seq)
       (map attributes->tag)))

(declare TagMap)

(s/defschema TagNode
  {:tag String
   :sub-keys (s/recursive #'TagMap)
   s/Keyword s/Any})

(s/defschema TagMap
  {String {String TagNode}})

(s/defn tag-tree :- TagMap
  ([tags] (tag-tree (constantly {}) tags))
  ([attrs-fn tags]
     ((fn tag-tree* [prefix attribute-lists]
        (->> attribute-lists
             (group-by (comp first first))
             (map-vals (fn->> (group-by (comp second first))
                              (map-vals (fn [l]
                                          (let [new-prefix (str (when prefix (str prefix +level-split+))
                                                                (attributes->tag (take 1 (first l))))]
                                            (merge
                                             {:tag new-prefix
                                              :sub-keys (tag-tree* new-prefix (keep next l))}
                                             (attrs-fn new-prefix)))))))))
      nil
      (mapv tag->attributes tags))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Working with type:tag as strings, e.g.user:offensive

(s/defn ^:always-validate split-typed-tag :- (s/pair TagType "tag-type" String "tag-value")
  [typed-tag :- String]
  (let [[t s] (.split typed-tag ":" 2)]
    [(keyword t) s]))

(s/defschema TypedTag
  (s/both String
          (s/pred split-typed-tag 'typed-tag?)))

(s/defn typed-tag :- TypedTag
  [tag-type :- TagType tag-value :- String]
  (str (name tag-type) ":" tag-value))

(set! *warn-on-reflection* false)
