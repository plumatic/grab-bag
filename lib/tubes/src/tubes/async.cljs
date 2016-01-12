(ns tubes.async)

(defn wait* [fs cb]
  (if (empty? fs)
    (cb)
    (let [semaphore (atom (count fs))]
      (doseq [f* fs]
        (f* #(when (zero? (swap! semaphore dec))
               (cb)))))))

(defn map* [f* coll cb]
  (let [res (js/Array.)
        err (js/Array.)]
    (wait*
     (for [[index item] (map-indexed vector coll)]
       #(f* item (fn [item-res item-err]
                   (aset err index item-err)
                   (aset res index item-res)
                   (%))))
     #(cb res (when (some boolean err) err)))))

(defn parallel* [m cb]
  (map* (fn [[k f*] cb]
          (f* #(cb [k %])))
        m
        #(cb (into {} %1) (into {} %2))))

(defn memoize* [f*]
  (let [cache (atom {})]
    (fn [& args-and-cb]
      (let [[args cb] ((juxt butlast last) args-and-cb)]
        (if-let [results (@cache args)]
          (cb results)
          (apply f* (concat args
                            [(fn [res]
                               (swap! cache assoc args res)
                               (cb res))])))))))

(defn with-only-one-in-flight*
  "Wraps an async function so that if a request to f*
   comes in while another is executing, the last request
   is cancelled (the request is returned by f*) according
   to the passed in abort function"
  [abort f*]
  (let [in-flight-atom (atom nil)]
    (fn [& args-and-cb]
      (when-let [in-flight @in-flight-atom]
        (abort in-flight))
      (let [[args cb] ((juxt butlast last) args-and-cb)
            req (apply f* args (fn [resp]
                                 (reset! in-flight-atom nil)
                                 (cb resp)))]
        (reset! in-flight-atom req)))))

(defn sync-compose* [process-result f*]
  (fn [& args-and-cb]
    (let [[args cb] ((juxt butlast last) args-and-cb)]
      (apply f* (concat args [(comp cb process-result)])))))

(defprotocol PAsyncSeq
  (take* [this n complete]
    "complete takes one argument `data`,
     where `data` is a seq of at most n items.")
  (depleted? [this])
  (busy? [this]))

(def +empty-async-seq+
  (reify tubes.async/PAsyncSeq
    (take* [this n complete] (complete nil))
    (depleted? [this] true)
    (busy? [this] false)))

(defn refilling-buffer
  "next-fn*: takes argument from complete callback (see above) and
   second arg is async-callback with items"
  [next-fn* threshold initial-buffer]
  (let [buffer (atom (into [] initial-buffer))
        next-args (atom [])
        next-in-flight? (atom false)
        has-next? (atom true)
        current-request (atom nil)
        next-complete (fn [this new-data]
                        (reset! next-in-flight? false)
                        (reset! next-args [])
                        (when-not (seq new-data)
                          (reset! has-next? false))
                        (swap! buffer concat new-data)
                        (when-let [[n complete] @current-request]
                          (reset! current-request nil)
                          (take* this (if @has-next? n (count this)) complete)))
        next!* (fn [this]
                 (reset! next-in-flight? true)
                 (next-fn* @next-args #(next-complete this %)))]
    (reify

      clojure.core/ICounted
      (-count [this] (count @buffer))

      tubes.async/PAsyncSeq
      (take* [this n complete]
        (assert (not @current-request))
        ;; TODO(cp): uncomment once activity-feed ready
        #_(assert (not (depleted? this)))
        (if (<= n (count this))
          (let [have-now (take n @buffer)]
            (swap! buffer #(drop n %))
            (let [[unused next-arg] (complete have-now)]
              (reset! buffer (concat unused @buffer))
              (swap! next-args conj next-arg)
              (when (< (count this) threshold)
                (next!* this))))
          (do
            (reset! current-request [n complete])
            (when-not @next-in-flight?
              (next!* this)))))

      (depleted? [this]
        (and (not @has-next?) (= (count this) 0)))

      (busy? [this] @current-request))))
