(ns domain.docs.products
  "Schemas for the relationship between a purchasable product and a doc that mentions it.

  Mentions
  ===========
  The full datum stored in the list of :products attached to a doc.
   - source: Keyword. The service linked to, like :amazon or :itunes
   - url: String. The link to the item, like http://www.amazon.com/gp/product/B004WULC3I/
   - key: String. The source-based ID of the item, like B004WULC3I
   - referral-type: :link or :internal. :link means the relevant item's product page *is*
     the current doc; :internal means the product page was linked to within the current doc
   - in-div: Boolean. True if this link was found in the doc's read-div; false otherwise
   - highlights: List of strings. If there's only one product linked on the whole page,
     this field is filled with all of the <li> elements on the page.

  Products
  ===========
  Contains whatever information we can get from the relevant source's API."
  (:use plumbing.core)
  (:require [schema.core :as s]
            [schema.experimental.abstract-map :as abstract-map]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Representing money

(s/defschema Price
  (s/conditional
   #(= (:currency %) "unknown") {:currency (s/eq "unknown")
                                 :formatted s/Str}
   :else {:currency s/Str
          :amount Number}))

(s/defn ^:always-validate make-price :- Price
  ([formatted :- s/Str]
     {:currency "unknown" :formatted formatted})
  ([curr :- s/Str amt :- Number]
     {:currency curr :amount amt}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Old schemas

(s/defschema AmazonAttributes
  {s/Keyword [{:attrs s/Any :content (s/cond-pre s/Str (s/recursive #'AmazonAttributes))}]})

(s/defschema OldAmazon
  "The Amazon product data stored in the index before February 2015."
  {:url s/Str
   :type (s/enum :link :internal)
   :price (s/maybe s/Str)
   :title s/Str
   :item-attributes AmazonAttributes})

(s/defschema OldItunes
  "The iTunes product data stored in the index before February 2015."
  {:key s/Str
   :source (s/eq :itunes)
   :name s/Str
   :type s/Str
   :url s/Str
   (s/optional-key :in-div) (s/eq true)
   (s/optional-key :highlights) [s/Str]})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Current schemas

(s/defschema Mention
  (abstract-map/abstract-map-schema
   :source
   {:url s/Str
    :key (s/maybe s/Str)
    :referral-type (s/enum :link :internal)
    :in-div (s/maybe s/Bool)
    :highlights [s/Str]}))

(s/defschema AmazonProduct
  {:price (s/maybe Price)
   :name s/Str
   :item-attributes (s/maybe AmazonAttributes)
   s/Any s/Any})

(s/defschema ItunesProduct
  {:name s/Str
   :type s/Str
   :genres [s/Str]
   :price (s/maybe Price)
   :rating (s/maybe {:num-ratings long :avg-rating Number})
   s/Any s/Any})

(abstract-map/extend-schema
 AmazonMention Mention
 [:amazon]
 {:product AmazonProduct})

(abstract-map/extend-schema
 ItunesMention Mention
 [:itunes]
 {:product ItunesProduct})

(s/defschema Extractor
  "A map containing enough information to identify a product from an external service."
  (abstract-map/abstract-map-schema
   :source
   {:key s/Str}))

(abstract-map/extend-schema
 AmazonExtractor Extractor
 [:amazon]
 {})

(abstract-map/extend-schema
 ItunesExtractor Extractor
 [:itunes]
 {:country s/Str})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Utility functions

(s/defn mention-source :- s/Keyword
  [m :- Mention]
  (safe-get m :source))

(s/defn mention-from? :- s/Bool
  [m :- Mention src :- s/Keyword]
  (= (mention-source m) src))

(defn amazon-mention? [m]
  (mention-from? m :amazon))
(defn itunes-mention? [m]
  (mention-from? m :itunes))

(s/defn arrange-by-sources :- {s/Keyword Mention}
  [ms :- [Mention]]
  (or (apply merge-with (comp vec concat)
             (map (fn [m] {(mention-source m) [m]}) ms))
      {}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Coercing legacy formats

(defnk old-amazon->reference :- AmazonMention
  [url type title price {item-attributes {}}] ;- OldAmazon
  {:source :amazon
   :url url
   :key nil
   :referral-type type
   :in-div nil
   :highlights []
   :product {:price (when price (make-price price))
             :name title
             :item-attributes item-attributes}})

(defnk old-itunes->reference :- ItunesMention
  [name type key url {in-div false} {highlights nil}] ;- OldItunes
  {:url url
   :key key
   :source :itunes
   :referral-type :internal
   :in-div in-div
   :highlights highlights
   :product {:name name
             :type type
             :genres []
             :price nil
             :rating nil}})

(defn merge-products-commerce [external-info]
  (letk [[products {commerce nil}] external-info]
    (if (contains? external-info :commerce)
      (-> external-info
          (assoc :products (mapv old-itunes->reference products))
          (?> commerce (update-in [:products] conj (old-amazon->reference commerce)))
          (update-in [:products] not-empty)
          (dissoc :commerce))
      external-info)))
