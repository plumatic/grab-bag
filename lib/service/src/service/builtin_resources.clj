(ns service.builtin-resources
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [clojure.tools.nrepl.server :as nrepl-server]
   [plumbing.error :as err]
   [plumbing.graph :as graph]
   [plumbing.observer :as plumbing-observer]
   [plumbing.resource :as resource]
   [aws.elb :as elb]
   [email.sendgrid :as sendgrid]
   [crane.config :as config]
   [store.bucket :as bucket]
   [store.mongo :as mongo]
   [store.s3 :as s3]
   [store.snapshots :as snapshots]
   [web.pubsub :as pubsub]
   [service.logging :as logging]
   [service.nameserver :as nameserver]
   [service.observer :as observer]))


(defn- remote? [env] (config/remote? env))
(defn- local? [env] (= env :local))
(defn- test? [env] (= env :test))

(defn- s3-bucket [ec2-keys name]
  (bucket/bucket (assoc ec2-keys :type :s3 :name name)))

(defn- mem-bucket [] (bucket/bucket {:type :mem}))

(defnk ^:private snapshot-store [env ec2-keys [:instance service-name]]
  (if (remote? env)
    (let [b (snapshots/snapshot-store ec2-keys service-name)]
      (comment (println "deleting old snapshots...")
               (s3/delete-all b)
               (println "done"))
      b)
    (mem-bucket)))

(defnk ^:private get-snapshot-store [[:instance service-name] env ec2-keys snapshot-store]
  (fn [other-service-name]
    (if (and (local? env)
             (= service-name other-service-name))
      snapshot-store
      (snapshots/snapshot-store ec2-keys other-service-name))))

(defnk ^:private log-data-store [env]
  (if (remote? env)
    (mongo/mongo-log-data-store
     {:host  (throw (Exception. "MONGO HOST")) :port (throw (Exception. "MONGO PORT"))}
     (throw (Exception. "MONGO USER"))
     mongo/+wc-normal+ #_ mongo/+wc-safe+)
    (mongo/test-log-data-store)))

(defnk ^:private nameserver [env ec2-keys [:instance service-name addresses]]
  (nameserver/nameserver-resource
   (if-not (test? env)
     (s3-bucket ec2-keys "grabbag-nameserver")
     (mem-bucket))
   (when (or (remote? env)
             (test? env))
     service-name)
   addresses
   {:stale-s 1200
    :host-key (if (remote? env) :private :public)}
   60))

(defnk ^:private pubsub-stats [observer]
  (plumbing-observer/counter observer :counts))

(defnk ^:private send-email [env]
  (if true #_ (= env :test) ;; email must be configured for other envs
      (let [a (atom [])]
        (fn fake-send-email [& [args]]
          (if (empty? args)
            (let [emails @a] (reset! a []) emails)
            (swap! a conj args))))
      (let [session (sendgrid/session
                     {:host "smtp.sendgrid.net"
                      :port 465
                      :password (throw (Exception. "PASSWORD"))
                      :user (throw (Exception. "USER"))})]
        (fnk send-email [& args]
          (-> args
              (assoc :session session)
              (update-in [:text] #(if (string? %) % (with-out-str (pprint/pprint %))))
              sendgrid/send)))))

;; in theory, server is java.lang.Closeable so it should be shutdown automagically.
;; in reality, nothing seems to make it shutdown.
(defnk ^:private nrepl-server [env {swank-port nil}]
  (when-not (= env :test)
    (when swank-port (nrepl-server/start-server :port swank-port))))

(defnk ^:private elb-manager [env ec2-keys [:service elb-names] {pre-shutdown-hook nil}]
  (when (remote? env)
    (do (elb/elb-action ec2-keys elb-names "Added" elb/add)
        (reify
          resource/PCloseable
          (close [this]
            (when pre-shutdown-hook
              (err/?error "Error in pre-shutdown-hook" (pre-shutdown-hook)))
            (elb/elb-action ec2-keys elb-names "Removed" elb/remove))))))

;; NOTE: observer is a funny resource, in that it has special-case
;; logic in impl to construct sub-observer.

(def +builtin-required-resources+
  (graph/graph
   :snapshot-store     snapshot-store
   :nameserver         nameserver
   :log-data-store     log-data-store
   :nrepl-server       nrepl-server
   :observer-bundle    observer/observer-bundle
   :observer           observer/observer-resource
   :send-email         send-email
   :logging-resource   logging/logging-resource))

(def +builtin-optional-resources+
  (graph/graph
   :pubsub             pubsub/pubsub-resources
   :subscriber*        (graph/instance pubsub/subscribers-resources [[:instance [:addresses [:private host]]]]
                         {:server-host host})
   :publisher*         pubsub/publishers-resources
   :pubsub-stats       pubsub-stats
   :get-snapshot-store get-snapshot-store))

(def +builtin-final-resources+
  (graph/graph
   :elb-manager elb-manager))
