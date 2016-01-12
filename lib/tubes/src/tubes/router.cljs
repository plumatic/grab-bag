(ns tubes.router
  (:require [clojure.string :as str]))

(def ^:dynamic routes (atom []))
(def popped (atom false))

(def starting-slash-pattern #"^/")
(def slash-pattern #"/")
(def param-pattern #"^:([^/]+)")
(def param-match-pattern #"([^/]+)")

(defn- strip
  [s r]
  (str/replace s r ""))

(defn- map-exp
  [s]
  (if (re-matches param-pattern s)
    [(strip s #"^:") param-match-pattern]
    s))

(defn- tokenize
  "Takes a route string and creates a vector of strings/vectors.
  Example: '/news/:feed' -> ['news' ['feed' #'.*']]"
  [route]
  (let [route (strip route starting-slash-pattern)]
    (mapv map-exp (str/split route slash-pattern))))

(defn- get-route-exp
  [tok-route]
  (re-pattern
   (reduce-kv
    (fn [r k v]
      (let [v (if (vector? v) "([^\\/]+)" v)]
        (str r "\\/" v)))
    ""
    tok-route)))

(defn- get-token-vec
  [tok-route]
  (mapv first (filterv vector? tok-route)))

(defn match-route
  [pattern route]
  (re-matches (:exp route) pattern))

(defn get-params
  "Extract params given pattern (a regex) and route. See tokenize."
  [pattern route]
  (let [matches (rest (match-route pattern route))
        tokens (:tok route)]
    (into {}
          (map
           (fn [token match]
             [(keyword token) (js/decodeURIComponent match)])
           tokens matches))))

(defn find-route
  [pattern]
  (-> (partial match-route pattern)
      (filter @routes)
      first))

(defn make-route
  [route action]
  (swap! routes conj
         (let [tok-route (tokenize route)]
           {:str route
            :exp (get-route-exp tok-route)
            :tok (get-token-vec tok-route)
            :act action})))

(defn dispatch
  [pattern previous-path & [state]]
  (let [pattern (str/replace pattern #"[#|?].*$" "")]
    (when-let [route (find-route pattern)]
      (let [action (:act route)
            params (get-params pattern route)]
        (action (assoc (merge state params) :previous-path previous-path))))))

(defn pathname []
  (.-pathname js/window.location))

(defn navigate! [route]
  (let [previous-path (pathname)]
    (.replaceState js/window.history
                   (js* "{scrollTop: document.body.scrollTop}") nil previous-path)
    (.pushState js/window.history nil nil route)
    (dispatch route previous-path)))

(defn ignore-first-til-after-load! [listen! f pred]
  "Before load, ignore the first event if (pred).
   After load, or after the first event, pretend we did (listen! f) all along"
  (listen! #(do (when-not (pred)
                  (f %))
                (listen! f)))
  (set! js/window.onload (fn [] (js/setTimeout #(listen! f)))))

(defn init []
  (let [init-path (pathname)]
    ;; some browsers fire popstate on page load, some don't
    ;;   https://developer.mozilla.org/en-US/docs/DOM/window.onpopstate
    ;; so ignore the first popstate if it's before the end of the load event,
    ;; if it doesn't change the page
    ;; (we don't want to ignore the popstate if a link to another page is
    ;;  clicked before the page finishes loading, or after that first spurious
    ;;  one, or after the load event when it must've resulted from user action)
    (ignore-first-til-after-load! #(set! js/window.onpopstate %)
                                  #(dispatch (pathname) nil
                                             (when-let [state (.-state %)]
                                               (js->clj state :keywordize-keys true)))
                                  #(identical? (pathname) init-path))))
