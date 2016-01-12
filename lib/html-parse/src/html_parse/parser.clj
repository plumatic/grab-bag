(ns html-parse.parser
  (:refer-clojure :exclude [ancestors descendants])
  (:use plumbing.core)
  (:require
   [clojure.string :as str])
  (:import
   java.io.StringReader java.io.StringWriter java.net.URL
   javax.xml.parsers.DocumentBuilderFactory
   [javax.xml.transform TransformerFactory]
   javax.xml.transform.OutputKeys
   javax.xml.transform.dom.DOMSource
   javax.xml.transform.stream.StreamResult
   org.apache.commons.lang.StringEscapeUtils
   org.ccil.cowan.tagsoup.Parser
   [org.w3c.dom Attr Document Element Node]
   [org.xml.sax InputSource]))


(set! *warn-on-reflection* true)

(defn ^URL url
  "Creates a URL. Returns nil if the url specifies and unkown protocol."
  [link]
  (try (if (instance? URL link) link
           (URL. link))
       (catch java.net.MalformedURLException _ nil)))

(defn relative-url
  "Creates a URL from a parent URL and a possibly-relative child.  Returns nil on url parse exception."
  [context link]
  (try (if (instance? URL link)
         link
         (URL. (url context) ^String link))
       (catch java.net.MalformedURLException _ nil)))

(defn host
  "Return the lowercased hostname for the given URL, if applicable."
  [^URL u]
  (when u (if-let [h (.getHost u)]
            (.toLowerCase h))))

;; TODO: use URI not URL.
;; jacked from nutch: Regex patern to get URLs within a plain text.
;; http://www.truerwords.net/articles/ut/urlactivation.html
;; URLs from plain text using Regular Expressions.
;; http://wiki.java.net/bin/view/Javapedia/RegularExpressions
;; http://regex.info/java.html @author Stephan Strittmatter - http://www.sybit.de
(def url-pattern
  #"([A-Za-z][A-Za-z0-9+.-]{1,120}:[A-Za-z0-9/](([A-Za-z0-9$_.+!*,;/?:@&~=-])|%[A-Fa-f0-9]{2}){1,333}(#([a-zA-Z0-9][a-zA-Z0-9$_.+!*,;/?:@&~=%-]{0,1000}))?)")

(defn url-seq [^String t]
  (->>
   (re-seq url-pattern t)
   (map (comp url #(.trim ^String %) first))
   (remove nil?)))


(defn dom [source]
  "html string -> dom using TagSoup.
   the features we set on the parser come from different implementations that I found in nutch, HtmlParser, as well as other parsers."
  (when source
    (try
      (let [result (org.apache.xalan.xsltc.trax.SAX2DOM.)
            input (if (instance? java.net.URL source)
                    (.openStream ^java.net.URL source)
                    (StringReader. ^String source))
            parser (doto (Parser.)
                     (.setContentHandler result)
                     (.setFeature Parser/namespacesFeature false)
                     (.setFeature Parser/namespacePrefixesFeature false)
                     (.setFeature Parser/bogonsEmptyFeature false)
                     (.parse (InputSource. ^java.io.Reader input)))]
        (.getDOM result))
      (catch org.w3c.dom.DOMException _ )
      (catch java.io.IOException _ )))) ;;pushback buffer overflow

(defn element? [^Node node]
  (and node (= (long (.getNodeType node)) (long Node/ELEMENT_NODE))))

(defn text-node? [^Node node]
  (and node
       (= (long (.getNodeType node)) (long Node/TEXT_NODE))))

(defn document-node? [^Node node]
  (and node
       (= (long (.getNodeType node)) (long Node/DOCUMENT_NODE))))

(defn node-name [^Node node] (.getNodeName node))

(defn nodelist-seq
  ([^org.w3c.dom.NodeList node-list]
     (nodelist-seq node-list nil))
  ([^org.w3c.dom.NodeList node-list ^String name]
     (filter identity
             (for [i (range 0 (.getLength node-list))
                   :let [node ^Node (.item node-list (int i))]]
               (when (or (nil? name) (.equalsIgnoreCase name (node-name node)))
                 node)))))

(defn children
  "children of current node"
  ([^Node n]
     (children n nil))
  ([^Node n ^String name]
     (nodelist-seq (.getChildNodes n) name)))

(defn depth
  "depth from the current node. use this to throw away crazy deep doms."
  ([max-depth ^Node n]
     (depth max-depth 0 n))
  ([max-depth curr-depth ^Node n ]
     (if (= curr-depth max-depth)
       (throw (RuntimeException. "crazy deep dom"))
       (let [c (children n)]
         (if (empty? c)
           curr-depth
           (reduce max (map (partial depth max-depth (inc curr-depth)) c)))))))

(defn nth-child [^Node n i]
  (.item (.getChildNodes n) i))

(defn child-index [^Node c]
  (when-let [p (.getParentNode c)]
    (let [cn (.getChildNodes p)]
      (loop [i (dec (.getLength cn))]
        (cond (neg? i) (throw (RuntimeException. "Child not found"))
              (identical? c (.item cn i)) i
              :else (recur (dec i)))))))

(defn parent [^Node n] (.getParentNode n))

(defn ancestors [^Node n]
  (->> (.getParentNode n)
       ancestors
       (cons n)
       (when n)))

(defn descendants [n]
  (->> (children n)
       (mapcat descendants)
       (cons n)))

(defn strict-descendants [^Node n]
  (mapcat descendants (children n)))

(defn least-common-ancestor [n1 n2]
  (->> (map vector (reverse (ancestors n1)) (reverse (ancestors n2)))
       (take-while (partial apply =))
       last first))


(defn attr-keys [^Node n]
  (when-let [attrs (.getAttributes n)]
    (for [i (range (.getLength attrs))]
      (.getName ^Attr (.item attrs (int i))))))

(defn attr [^Node n ^String a]
  (when-let [attrs (.getAttributes n)]
    (when-let [att ^Attr (.getNamedItem attrs a)]
      (.getValue att))))

(defn delete-attr! [^Node n ^String a]
  (.removeAttribute ^Element n a))

(defn attr-map
  "returns node attributes as map of keyword attribute keys to str value"
  [^Node n]
  (when-let [attrs (.getAttributes n)]
    (into {}
          (for [i (range (.getLength attrs))
                :let [^Attr item (.item attrs i)]
                :when (.getSpecified item)]
            [(-> item (.getName) keyword) (.getTextContent item)])))) ;; TODO: inconsistent with .getValue above.

;;TODO: script thing still not working?
(defn extract-text [^Node n]
  (if (not (text-node? n))
    ""
    (.getNodeValue n)))

(defn elements
  "gets the elements of a certian name in the dom
   (count (elements doc \"div\")) -> 199"
  [p ^String t]
  (when p
    (let [node-list (condp #(isa? %2 %1) (class p)
                      Document (.getElementsByTagName ^Document p t)
                      Element (.getElementsByTagName ^Element p t))]
      (filter identity (nodelist-seq node-list)))))

(defn strip-from-dom
  [^Document d es]
  (doseq [^Node e es]
    (.removeChild (.getParentNode e) e))
  (.normalize d)
  d)

(defn strip-tags [d & tags]
  (if (or (not tags)
          (empty? tags))
    d
    (recur (strip-from-dom d (elements d (first tags)))
           (rest tags))))

;;TODO: WTF is up with the required calling of strip-non-content twice?
;;something about the side effects happening in the stip tags or stip from dom fns?
(defn strip-non-content [d]
  (assert d)
  (let [f #(strip-tags %
                       "script" "style" "form"
                       "object" "iframe")]
    (f (f d))))

(defn expand-relative-urls! [link dom]
  (let [url (url link)]
    (doseq [[t ^String a] {"a" "href" "link" "href" "img" "src"}]
      (doseq [^Element e (elements dom t)]
        (when-let [u (.getAttribute e a)]
          (when-let [expanded (relative-url url u)]
            (.setAttribute e a (str expanded))))))
    dom))

(defn head [^Document d]
  (when-let [h (.getElementsByTagName d "head")]
    (.item h 0)))

(defn do-children [^Node n f]
  (if (not (and n (.hasChildNodes n)))
    []
    (doall (map f (children n)))))

(defn walk-dom
  "recursively walk the dom.
  combine: result of visiting a single node & rest -> combines them as
  appropriate for the representational structure
  init: initilial value for combine
  visit: visits one node and produces a result. if result of visit
  is :kill, then do not recurse on node and ignore node"
  ([d visit combine]
     (let [extractor (fn extract [n]
                       (let [r (visit n)]
                         (when-not (= r :kill)
                           (combine r
                                    (do-children n extract)))))]
       (extractor d))))

(defn text-from-dom
  "recursively get the text content from Nodes.
   inspired by: http://www.prasannatech.net/2009/02/convert-html-text-parser-java-api.html"
  [d]
  (let [buffer (StringBuffer.)
        walk (fn walk [^Node n]
               (if (text-node? n)
                 (.append buffer (.getNodeValue n))
                 (doseq [c (children n)]
                   (walk c))))]
    (walk d)
    (str buffer)))

(defn unescape-html
  "Convert HTML entities to proper Unicode characters."
  [^String t]
  (StringEscapeUtils/unescapeHtml t))

(defn replace-unicode-control
  [^String s]
  (when s
    (-> s
        (.replace \u2028 \u000a)
        (.replace \u2029 \u000a))))

(defn strip-space [^String s]
  (.trim (.replaceAll s "[\n \t]{3,}" "\n")))

(defn dom->hiccup [^Node n]
  (cond
   (document-node? n) (-> n children singleton (doto (assert "not singleton")) dom->hiccup)
   (text-node? n) (-> n extract-text strip-space not-empty)
   (element? n)
   (->> (map dom->hiccup (children n))
        (cons (-> n attr-map not-empty))
        (cons (keyword (node-name n)))
        (remove nil?)
        vec)))

(defn html-title [d]
  (when-let [t (or (first (elements d "title"))
                   (first (elements d "h1")))]
    (text-from-dom t)))

(defn html-str
  "return the html string representing the node; should
   be semantically equivlaent but attribute order and other
   things like spacing and formatting may be gone."
  [^Node node]
  (let [^Document d (if (instance? org.w3c.dom.Document node)
                      node
                      (.getOwnerDocument node))]
    (-> d
        ^org.w3c.dom.ls.DOMImplementationLS (.getImplementation)
        (.createLSSerializer)
        (.writeToString node))))

(defn html-str2 [d]
  (let
      [out (StreamResult. (StringWriter.))
       tf (doto
              (.newTransformer (TransformerFactory/newInstance))
            (.setOutputProperty
             OutputKeys/OMIT_XML_DECLARATION "yes")
            (.setOutputProperty
             OutputKeys/METHOD "xml")
            (.setOutputProperty OutputKeys/ENCODING "UTF-8"))]
    (.transform tf (DOMSource. d) out)
    (.toString (.getWriter out))))

(set! *warn-on-reflection* false)
