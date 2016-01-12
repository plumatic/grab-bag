(ns web.server
  (:use plumbing.core)
  (:require
   [ring.adapter.jetty :as jetty]
   [plumbing.logging :as log]
   [plumbing.resource :as resource])
  (:import
   [java.net ServerSocket]
   [org.mortbay.jetty Server]
   [org.mortbay.jetty.nio SelectChannelConnector]))


(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: helpers

(defn get-available-port []
  "returns a currently unused port"
  (let [ss (ServerSocket. 0)
        port (.getLocalPort ss)]
    (.close ss)
    port))


(defnk client-ip [remote-addr headers :as request]
  (get headers "x-forwarded-for" remote-addr))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public: creating servers

(defn- create-server
  "Construct a Jetty Server instance."
  [options]
  (let [connector (doto (SelectChannelConnector.)
                    (.setHeaderBufferSize 262144) ;; account for huge cookies from iOS client, can be deleted later.
                    (.setPort (options :port 80))
                    (.setHost (options :host))
                    (.setAcceptQueueSize 256))
        server    (doto (Server.)
                    (.addConnector connector)
                    (.setSendDateHeader true))]
    (when-let [t (:shutdown-timeout options)]
      (.setGracefulShutdown server (int t)))
    (when-let [n (:num-threads options)]
      (log/infof "Using fixed-size thread pool %d" n)
      (doto server
        (.setThreadPool (org.mortbay.thread.QueuedThreadPool. (int n)))))
    server))

(defn ^Server run-jetty
  "Serve the given handler according to the options.
  Options:
    :configurator   - A function called with the Server instance.
    :port
    :num-threads    - num threads for fixed pool
    :host
    :join?          - Block the caller: defaults to true.
    :ssl?           - Use SSL.
    :ssl-port       - SSL port: defaults to 443, implies :ssl?
    :keystore
    :key-password
    :truststore
    :trust-password
    :shutdown-timeout - Allow requests to finish for this many ms."
  [handler options]
  (let [^Server s (create-server (dissoc options :configurator))]
    (when-let [configurator (:configurator options)]
      (configurator s))
    (doto s
      (.addHandler (@#'jetty/proxy-handler handler))
      (.start))
    (when (:join? options true)
      (.join s))
    s))

(extend-protocol resource/PCloseable
  Server
  (close [this] (.stop ^Server this)))

(defnk server-resource
  [root-handler
   {configurator nil} {port nil} {num-threads nil} {host nil} {join? nil} {shutdown-timeout nil}
   :as jetty-opts]
  (org.mortbay.log.Log/setLog nil)
  (run-jetty root-handler (dissoc jetty-opts :root-handler)))

(set! *warn-on-reflection* false)
