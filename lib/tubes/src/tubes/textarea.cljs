(ns tubes.textarea
  (:require
   [clojure.string :as str]
   [dommy.core :as dommy]
   [tubes.text-metrics :as text-metrics]))

(defn listen-edit! [textarea f]
  (let [prev-value (atom (.-value textarea))
        on-edit-fn
        #(let [new-value (.-value textarea)]
           (when-not (identical? new-value @prev-value)
             (reset! prev-value new-value)
             (f new-value)))
        deferred-on-edit-fn #(js/setTimeout on-edit-fn 0)]
    (dommy/listen! textarea
                   :input on-edit-fn
                   :cut deferred-on-edit-fn
                   :paste deferred-on-edit-fn
                   :drop deferred-on-edit-fn
                   :keydown deferred-on-edit-fn
                   :keypress deferred-on-edit-fn)))

(defn invisible-clone [node]
  (doto (.cloneNode node)
    (dommy/remove-attr! :id)
    (dommy/set-style! :position "absolute"
                      :visibility "hidden"
                      :left 0
                      :top 0
                      :tabindex -1)
    (dommy/insert-after! node)))

(defn strip-newlines [text]
  (str/replace text #"[\n\r]" " "))

(defn truncate [elem num-lines]
  (let [max-width (dommy/px elem :width)
        break-set (set " ")
        min-hang 6
        clone (invisible-clone elem)]
    (dommy/set-style! clone :white-space "nowrap")
    (->> (text-metrics/clamp-text
          (text-metrics/elem-text-measurer clone)
          (-> elem .-innerHTML strip-newlines)
          max-width
          break-set
          num-lines
          min-hang)
         (str/join " ")
         (set! (.-innerHTML elem)))))

(defn move-caret-to-end
  "TODO: make this more cross-browser compatible
   TODO: move to more generic input lib"
  [elem]
  (let [i (-> elem .-value .-length)]
    (set! (.-selectionStart elem) i)
    (set! (.-selectionEnd elem) i)))

(defn caret-position [textarea]
  (if-let [p (.-selectionStart textarea)] p 0))
