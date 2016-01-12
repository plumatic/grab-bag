(ns service.nameserver
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.error :as err]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [crane.config :as config]
   [store.bucket :as bucket]))

(def NameserverEntry
  {:started long
   :uptime long
   :timestamp long
   :period-s (s/maybe long) ;; maybe is for old services
   :addresses config/Addresses
   s/Keyword s/Any ;; for old services, remove once everyting redeployed.
   })


(defn nameserver-client [bucket & [opts]]
  (assert (every? #{:stale-s :host-key} (keys opts)))
  (merge
   {:bucket bucket
    :stale-s 1200
    :host-key  :private}
   opts))

(defn age-in-seconds [ms]
  (let [now (System/currentTimeMillis)
        s-diff (quot (- now ms) 1000)]
    (when (< s-diff 0)
      (log/warnf "Nameserver got timestamp %d seconds in the future" (- s-diff)))
    s-diff))

;; TODO: beware of clock skew
(defn lookup
  "Returns service info, which includes at minimum:
   :config    {}
   :hosts       {}
   :timestamp clj-time/now pub-time of record
   :age-s     age of record, integer seconds,
  or nil if no service exists with this name.
  Deletes stale records."
  [ns service-name]
  (let [{:keys [bucket stale-s]} ns]
    (err/with-ex (err/logger)
      #(when-let [v (bucket/get bucket service-name)]
         (assert (every? v [:addresses :timestamp]))
         (let [age (age-in-seconds (:timestamp v))]
           (if (< age stale-s)
             (assoc v :age age)
             (do (bucket/delete bucket service-name)
                 nil)))))))

(s/defn ^:always-validate lookup-address :- config/Address
  [ns service-name]
  (safe-get-in (lookup ns service-name) [:addresses (:host-key ns)]))

(s/defn lookup-host :- String
  [ns service-name]
  (safe-get (lookup-address ns service-name) :host))

(defn all-service-names
  "Return all service names, perhaps including stale ones."
  [n]
  (bucket/keys (:bucket n)))

(defn service-map [n]
  (into {}
        (for [nm (all-service-names n)
              :let [info (lookup n nm)]
              :when info]
          [nm info])))

(defn nameserver-publisher
  "Publish service data about in bucket at a fixed frequency, starting now.
   Returns a function that can be called to stop publications."
  [bucket service-name addresses & [period-s]]
  (let [period-s (or period-s 60)
        launch (millis)
        doit #(bucket/put
               bucket service-name
               (let [now (millis)]
                 (s/validate NameserverEntry
                             {:config nil ;; old code asserted this, TODO remove
                              :started launch
                              :uptime (- now launch)
                              :timestamp now
                              :period-s period-s
                              :addresses addresses})))
        e (parallel/schedule-work doit period-s)]
    (doit)
    #(parallel/shutdown e)))


(defn nameserver-resource
  "pass nil service-name for no pub"
  [bucket service-name addresses client-opts period-s]
  (resource/make-bundle
   (nameserver-client bucket client-opts)
   (when service-name
     (nameserver-publisher bucket service-name addresses period-s))))
