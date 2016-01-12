(ns tubes.event-emitter
  (:require
   [tubes.core :as tubes]))

(defprotocol PEventEmitter
  (listen! [this type key listener] [this type listener]
    "Adds `listener` to `type` listener map. Returns this")
  (unlisten! [this type key]
    "Removes listener defined by `key` from the `type` listener map. Returns this.")
  (fire [this type event]
    "Calls all `type` listener fns with `event`. Returns this."))

(defn wait-for-all! [event-emitters event on-complete & [init-vals]]
  (when init-vals (assert (= (count event-emitters) (count init-vals))))
  (let [init-vals (or init-vals (repeat (count event-emitters) false))
        state (atom (zipmap event-emitters init-vals))
        done? #(every? identity (vals @state))]
    (doseq [ee event-emitters]
      (listen! ee event (fn [valid?]
                          (swap! state assoc ee valid?)
                          (on-complete (done?)))))))

(defn event-emitter []
  (let [listeners (atom {})]
    (reify
      PEventEmitter
      (listen! [this type key listener]
        (swap! listeners assoc-in [type key] listener)
        this)
      (listen! [this type listener]
        (listen! this type (name (gensym "listener")) listener))
      (unlisten! [this type key]
        (swap! listeners tubes/dissoc-in [type key])
        this)
      (fire [this type event]
        (doseq [[_ listener] (get @listeners type)]
          (listener event))
        this))))

(defn listen-all! [event-emitter & specs]
  (assert (even? (count specs)))
  (doseq [[type listener] (partition 2 specs)]
    (listen! event-emitter type listener))
  event-emitter)

(defn listen-once! [event-emitter type key listener]
  (listen!
   event-emitter type key
   (fn [e]
     (unlisten! event-emitter type key)
     (listener e))))
