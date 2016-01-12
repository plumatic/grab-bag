(ns web.async-client
  (:use plumbing.core)
  (:require
   [plumbing.chm :as chm]
   [plumbing.error :as err]
   [plumbing.logging :as log]
   [plumbing.parallel :as parallel]
   [plumbing.resource :as resource]
   [web.client :as sync-client])
  (:import
   [java.net InetSocketAddress]
   [java.nio.charset Charset]
   [javax.net.ssl SSLContext SSLEngine]
   [org.jboss.netty.bootstrap ClientBootstrap]
   [org.jboss.netty.channel
    ChannelEvent ChannelFuture ChannelFutureListener ChannelHandlerContext
    ChannelPipelineFactory ChannelState ChannelStateEvent ChannelUpstreamHandler Channels
    ExceptionEvent MessageEvent]
   [org.jboss.netty.channel.group ChannelGroup DefaultChannelGroup]
   [org.jboss.netty.channel.socket.nio NioClientSocketChannelFactory]
   [org.jboss.netty.handler.codec.http
    DefaultHttpRequest HttpClientCodec HttpContentDecompressor HttpMethod HttpRequest HttpResponse HttpVersion]
   [org.jboss.netty.handler.ssl SslHandler]
   [org.jboss.netty.handler.timeout ReadTimeoutHandler]
   [org.jboss.netty.util HashedWheelTimer]
   [web GlobalTimeoutHandler HTTPBoundedChunkAggregator]))


(set! *warn-on-reflection* true)

;; http://www.ietf.org/rfc/rfc2616.txt -- headers keys are case insensitive
;; NOTE: We break with the sync-client in that headers are lowercase and keywords, not strings with uppercase

;; TODO: in case where callback called multiple times in simple-bounded-handler,
;; channel never closes.  We really need to figure out how to ensure channel closed.

(def ^Charset +utf-8+ (Charset/forName "UTF8"))

(defn simple-bounded-handler [^ChannelGroup  channel-group callback]
  (let [call-count (atom 0)
        callback (fn [^ChannelEvent e x]
                   (swap! call-count inc)
                   (if (> @call-count 1)
                     (log/debugf "Attempt to call channel callback more than once: %s %s %s" e (.getChannel e) x)
                     (callback x)))]
    (reify ChannelUpstreamHandler
      (^void handleUpstream [this ^ChannelHandlerContext ctx ^ChannelEvent e]
        (cond
         ;; message
         (instance? MessageEvent e)
         (callback e (.getMessage  ^MessageEvent e))

         ;; open
         (and (instance? ChannelStateEvent e) (= (.getState ^ChannelStateEvent e) ChannelState/OPEN) (= (.getValue ^ChannelStateEvent e) (Boolean/TRUE)))
         (.add channel-group (.getChannel ctx))

         ;; exception
         (instance? ExceptionEvent e)
         (do
           (let [ex (.getCause ^ExceptionEvent e)]
             (when-not (instance? java.net.ConnectException ex)
               ;; (log/infof "SBE2 %s %s %s" (.getChannel ^ExceptionEvent e) callback ex)
               (callback e {:error ex})))
           (.close (.getChannel ^ExceptionEvent e)))

         ;; pass
         :else
         (.sendUpstream ctx e)
         )))))

(def ^SSLContext +default-ssl+ (SSLContext/getDefault))

(defn ^SSLEngine client-ssl-engine []
  (doto (.createSSLEngine +default-ssl+)
    (.setUseClientMode true)))

(defn https? [^String scheme]
  (case (.toLowerCase scheme)
    "http" false "https" true))

(defn http-pipeline-factory [handler-fn ^HashedWheelTimer timer read-timeout global-timeout https? max-bytes]
  (reify ChannelPipelineFactory
    (getPipeline [this]
      (let [p (Channels/pipeline)]
        (when read-timeout
          (.addLast p "timeout" (ReadTimeoutHandler. timer (quot read-timeout 1000))))
        (when global-timeout
          (.addLast p "global-timeout" (GlobalTimeoutHandler. timer (quot global-timeout 1000))))
        (when https?
          (.addLast p "ssl" (SslHandler. (client-ssl-engine))))
        (doto p
          (.addLast "codec" (HttpClientCodec.))
          (.addLast "inflater" (HttpContentDecompressor.))
          (.addLast "aggregator" (HTTPBoundedChunkAggregator. max-bytes))
          (.addLast "handler" (handler-fn)))))))

(defnk async-http-client-resource [{num-boss-threads 3}
                                   {num-worker-threads (* 2 (.availableProcessors (Runtime/getRuntime)))}
                                   {connect-timeout 10000}
                                   {read-timeout 20000}
                                   {max-connections 2000}
                                   {max-domain-connections 10}
                                   {num-retries 2}
                                   {redirect-policy (fn [new-req] (< (count (:redirects new-req)) 10))}
                                   {user-agent sync-client/spoofed-http-user-agent}]
  (assert (or (zero? read-timeout) (>= read-timeout 1000))) ;; ms -- zero means no timeout.
  (let [boss-pool (parallel/cached-thread-pool)
        worker-pool (parallel/cached-thread-pool)
        timer (HashedWheelTimer.)
        cg (DefaultChannelGroup.)]
    (resource/make-bundle
     {:socket-factory (NioClientSocketChannelFactory. boss-pool worker-pool
                                                      (int num-boss-threads)
                                                      (int num-worker-threads))
      :timeout-timer timer
      :channel-group cg
      :connect-timeout connect-timeout
      :read-timeout read-timeout
      :num-retries num-retries
      :max-connections max-connections
      :redirect-policy redirect-policy
      :max-domain-connections max-domain-connections
      :open-connection-counts (java.util.concurrent.ConcurrentHashMap.)
      :headers {:user-agent user-agent
                :accept "*/*"
                :connection "close"
                :accept-encoding "gzip,deflate"}}
     (fn shutdown-async-client []
       (when-not (-> cg (.close) (.awaitUninterruptibly 1000))
         (log/warn "Not able to shutdown channel group"))
       (.stop timer)
       (parallel/two-phase-shutdown boss-pool)
       (parallel/two-phase-shutdown worker-pool)))))

(defn open-connection-count [client]
  (.size ^ChannelGroup (safe-get client :channel-group)))

(defn input-headers! [^HttpRequest req headers]
  (doseq [[k v] headers] (.setHeader req (name k) (name v)))
  req)

(defn lowercase-headers [m]
  (map-keys #(.toLowerCase ^String (name %)) m))

(defn merge-headers [& maps]
  (apply merge (map lowercase-headers maps)))

(defn coerce-output [^HttpResponse resp]
  {:status (-> resp .getStatus .getCode)
   :body (-> resp .getContent (.toString +utf-8+))
   :headers (map-keys keyword (lowercase-headers (into {} (.getHeaders resp))))
   })

(declare fetch)

(defn exec-callback [client m callback raw-resp]
  (try
    (if (:error raw-resp)
      (let [retries (or (:num-retries m) (:num-retries client))]
        (if (> retries 0)
          (fetch client (assoc m :num-retries (dec retries) :retry? true ) callback)
          (callback raw-resp)))
      (let [resp (coerce-output raw-resp)
            redirect (when (contains? sync-client/+url-redirects+ (:status resp))
                       (sync-client/relative-url (:url m) (get-in resp [:headers :location])))]
        (if (not redirect)
          (callback (assoc resp
                      :url (:orig-url m)
                      :redirects (:redirects m)))
          (let [new-req (assoc m
                          :retry? true
                          :url redirect
                          :redirects (conj (:redirects m []) [(-> resp :status str keyword) redirect]))]
            (if ((:redirect-policy client) new-req)
              (fetch client new-req callback)
              (callback {:error (RuntimeException. (format "redirects error %s %s" (:url new-req) (:redirects m))) :last-resp resp}))))))
    (catch Throwable t
      (callback {:error t :message "Error in exec-callback -- no retries."}))))

(defn http-request [method parsed-url headers]
  (input-headers!
   (DefaultHttpRequest.
     HttpVersion/HTTP_1_1
     (HttpMethod/valueOf method)
     (str (if (empty? (:uri parsed-url)) "/" (:uri parsed-url))
          (when-let [q (:query-string parsed-url)] (str "?" q))))
   headers))

(defn parsed-url->address [u]
  (InetSocketAddress. ^String (:host u) (int (or (:port u) (if (https? (:scheme u)) 443 80)))))

(def +any-domain+ ::any-domain)

(defn connection-count [client & [domain]]
  (get (safe-get client :open-connection-counts) (or domain +any-domain+) 0))

(defn- incer [m k v]
  (chm/update!
   m k
   (fn [ov]
     (let [nv (+ (or ov 0) v)]
       (when (> nv 0)
         nv)))))

(defn update-connection-count! [client domain delta]
  (let [cc (safe-get client :open-connection-counts)]
    (incer cc domain delta)
    (incer cc +any-domain+ delta)))

(def over-capacity-resp? #{:over-global-capacity :over-domain-capacity})

;; tODO: do we need to periodically reset connection counts to account for accumulated errors?
(defn ^ChannelFuture fetch [client m callback]
  (try
    (let [m (if (string? m) {:url m :method :get} m)
          m (merge {:orig-url (:url m)} m)
          b (ClientBootstrap. (safe-get client :socket-factory))
          u (sync-client/parse-url (:url m))]
      ;; (log/infof "In resolve fetch with %s %s %s %s %s %s"
      ;;            (:url m) (:retry? m) (:num-retries m) (:max-redirects m)
      ;;            (count (:redirects m)) (open-connection-count client))
      (or
       (and (not (:retry? m))
            (or #_(when-let [max-conns (:max-connections client)]
                    (when (> (open-connection-count client) max-conns)
                      :over-global-capacity))
                (when-let [max-conns (:max-connections client)]
                  (when (> (connection-count client) max-conns)
                    #_(log/infof "Over global capacity with new %s channelgroup %s"
                                 (connection-count client) (open-connection-count client))
                    :over-global-capacity))
                (when-let [max-conns (:max-domain-connections client)]
                  (when (> (connection-count client (:host u)) max-conns)
                    #_(log/infof "Over host capacity for host %s %s" (:host u) u)
                    :over-domain-capacity))))
       (do (update-connection-count! client (:host u) 1)
           (doto b
             (.setPipelineFactory (http-pipeline-factory
                                   #(simple-bounded-handler
                                     (safe-get client :channel-group)
                                     (fn [resp] (exec-callback client m callback resp)))
                                   (safe-get client :timeout-timer)
                                   (or (:read-timeout m) (:read-timeout client) 20000)
                                   (or (:global-timeout m) (:global-timeout client) 60000)
                                   (https? (:scheme u))
                                   (:max-bytes m -1)))
             (.setOption "tcpNoDelay" true)
             (.setOption "keepAlive" false)
             (.setOption "connectTimeoutMillis" (or (:connect-timeout m) (:connect-timeout client) 10000)))
           (let [cf (.connect b (parsed-url->address u))]
             (.addListener cf (reify ChannelFutureListener
                                (operationComplete [this  cf]
                                  (update-connection-count! client (:host u) -1)
                                  (if (.isSuccess cf)
                                    (.write (.getChannel cf)
                                            (http-request
                                             (name (:method m :get)) u
                                             (merge-headers (:headers client) (:headers m) {:Host (:host u)})))
                                    (exec-callback client m callback {:error (.getCause cf)})))))
             cf))))
    (catch Throwable t
      ;; (log/infof t)
      (exec-callback client m callback {:error t :message "Error in fetch"}))))


(defn resp->canonical-url [fetchable-url? resp]
  (err/?debug
   "Error resolving url"
   (when-not (:error resp)
     (let [^String cann-url (sync-client/resp->canonical-url resp)]
       (when (fetchable-url? cann-url)
         (String. cann-url))))))

(defn blacklist-safe-redirect-policy [fetchable-url? new-req]
  (and (< (count (:redirects new-req)) 10)
       (fetchable-url? (:url new-req))))

(defn safe-canonical-url [async-client ^String url callback fetchable-url? & [max-chars]]
  (when (and (fetchable-url? url)
             (.startsWith url "http"))
    (fetch async-client {:url (.replace url " " "%20")
                         :method :get
                         :max-bytes (or max-chars 10000)}
           #(callback (resp->canonical-url fetchable-url? %)))))

(set! *warn-on-reflection* false)
