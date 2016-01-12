(ns service.repl
  (:use plumbing.core)
  (:require
   [clojure.pprint :as pprint]
   [clojure.reflect :as reflect]
   [lazymap.core :as lazymap]
   [plumbing.error :as err]
   [crane.config :as crane-config]
   [store.bucket :as bucket]
   store.s3
   [store.snapshots :as snapshots]
   [service.graft :as graft]
   [service.nameserver :as nameserver]
   [service.remote-repl :as remote-repl]))


(def config
  (let [config (delay (crane-config/read-dot-crane))]
    (lazymap/lazy-hash-map
     :ec2-keys (select-keys @config [:key :secretkey])
     :snapshot-store (fn [service-name] (snapshots/snapshot-store @config service-name))
     :nameserver (nameserver/nameserver-client
                  (bucket/bucket (merge @config {:type :s3 :name "grabbag-nameserver"}))
                  {:stale-s 1200 :host-key :public}))))

(defn lookup-address [service-name]
  (nameserver/lookup-address (:nameserver config) service-name))

(defn s3-bucket  [name & [serialize-method key-prefix]]
  (bucket/bucket
   (merge (:ec2-keys config)
          {:type :s3 :name name}
          (if serialize-method {:serialize-method serialize-method} {})
          (if key-prefix {:key-prefix key-prefix} {}))))

(defn dynamo-bucket  [name & [serialize-method]]
  (bucket/bucket
   (merge (:ec2-keys config)
          {:type :dynamo :name name}
          (if serialize-method {:serialize-method serialize-method} {}))))

(defn latest-snapshot [service-name]
  (-> ((:snapshot-store config) service-name)
      snapshots/read-latest-snapshot))

(defn show [c & [no-ancestors?]]
  (pprint/print-table (sort-by :name (:members (reflect/reflect c :ancestors (not no-ancestors?))))))

(defn current-service [] (-> remote-repl/resources deref :service))

(defn update-service!
  [new-service-sub-graph]
  (swap! remote-repl/resources update-in [:service] graft/graft-service-graph new-service-sub-graph)
  :success)

(defn restart-from!
  "Graft current service, assuming you've recompiled the deploy and all code you care about.
   ks are keys to restart from in the service.  If no keys are provided, restart everything
   except nrepl server.  Provide deploy-ns as a keyword, or we default to the service type."
  [deploy-ns & ks]
  (let [[deploy-ns ks] (if (symbol? deploy-ns)
                         [deploy-ns ks]
                         [(symbol (str (safe-get-in @remote-repl/resources [:config :service :type]) ".deploy"))
                          (cons deploy-ns ks)])
        ks (if (seq ks) (set ks) #(not= % :nrepl-server))]
    (update-service! (aconcat (for [[k v] @(resolve (symbol (name deploy-ns) "service-graph"))]
                                (when (ks k) [k v]))))))

(defn init-logger! [& [level]]
  (err/init-logger! (or level :info)))

(defn my-defs
  "Return a list of all of the vars defined in the current namespace"
  []
  (let [ns-name (str "#'" (.getName *ns*) "/")]
    (keep (fn [v] (when (.contains (str v) ns-name) (.replaceAll (str v) ns-name ""))) (vals (ns-map *ns*)))))


;; to service?
;; needs to be available all over, and have access to everything
