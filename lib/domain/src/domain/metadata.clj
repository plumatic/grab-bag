(ns domain.metadata
  "Utilities to extract meta- and micro-data from hiccup-parsed dom elements"
  (:use plumbing.core)
  (:require
   [clojure.string :as str]
   [schema.core :as s]
   [plumbing.core-incubator :as pci]
   [plumbing.logging :as log]
   [plumbing.string :as string]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas and Constructors

(declare HiccupElement)

(s/defschema HiccupChild
  (s/conditional
   string? String
   :else (s/recursive #'HiccupElement)))

(s/defschema HiccupElement
  "A schema for validating Hiccup syntax. The attribute map is optional,
   but because schema doesn't support backtracking
   it must be implemented with a conditional schema."
  (s/conditional
   (comp map? second)
   [(s/one s/Keyword "tag")
    (s/one {s/Keyword String} "Attribute map")
    HiccupChild]

   :else
   [(s/one s/Keyword "tag") HiccupChild]))

(s/defschema Metadata
  {:meta-tags {(s/named String "meta-tag attribute name (not case-normalized)")
               (s/named [String] "values")}
   :micro-data (s/named [HiccupElement] "Elements tagged with microdata from the body of the dom")})

(s/defn ^:always-validate metadata :- Metadata
  [meta-tags micro-data]
  {:meta-tags meta-tags
   :micro-data micro-data})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(defn matches-tag? [tag dom]
  (and (vector? dom) (= tag (first dom))))

(defn micro-data? [dom]
  (when-let [attr (second dom)]
    (and (map? attr)
         ((some-fn :itemprop :property) attr))))

(defn filter-dom
  "Returns a seq of subtrees of the dom matching pred.
   If there are nested matches, only returns the largest."
  ([pred dom]
     (filter-dom pred [] dom))
  ([pred extracted dom]
     (cond
      (not (vector? dom)) extracted
      (pred dom) (conj extracted dom)

      :else (->> dom
                 next
                 (drop-while map?)
                 (reduce (partial filter-dom pred) extracted)))))

(defn elements
  "Return a lazy preorder traversal of elements from a hiccup form, normalized
   to always have attribute maps."
  [n]
  (when (vector? n)
    (let [normalized (if (map? (second n)) n (vec (concat [(first n) {}] (next n))))]
      (cons normalized
            (mapcat elements (drop 2 normalized))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(s/defn extract-meta :- Metadata
  "Walk hiccup-parsed dom, and extract all metadata"
  [dom]
  (let [meta-tag? (partial matches-tag? :meta)]
    (metadata
     (pci/group-map-by
      first second
      (for [head (filter-dom #(matches-tag? :head %) dom)
            [_ attr] (filter-dom meta-tag? head)
            :let [attr-name ((some-fn :property :itemprop :name) attr)
                  content (:content attr)]
            :when (and attr-name content)]
        [attr-name content]))
     (for [body (filter-dom #(matches-tag? :body %) dom)
           micro-data (filter-dom micro-data? body)]
       micro-data))))

(s/defn microdata-props
  "Extract a flat set of attribute maps for microdata, throwing away the content and other tags."
  [metadata]
  (->> metadata
       :micro-data
       (mapcat elements)
       (keep #(not-empty (select-keys (second %) #{:itemprop :itemtype :property :rel})))))

(defn comma-separated-tokens [^String s]
  (keep
   #(not-empty (str/lower-case (str/trim %)))
   (.split s ",")))

(s/defn keywords
  "Extract normalized, lowercased, split keywords from 'keywords' meta tag.
   Look for various capitalizations -- 90% are 'keywords', but also see 'KeyWords' and such."
  [metadata]
  (->> metadata
       :meta-tags
       (keep (fn [[k v]]
               (when (= (str/lower-case k) "keywords")
                 v)))
       aconcat
       (mapcat comma-separated-tokens)))
