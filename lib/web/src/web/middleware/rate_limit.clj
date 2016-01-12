(ns web.middleware.rate-limit
  "When the frequency of calls per session exceeds the rate limit, status code 429 is returned
   and further processing of the request is stopped."
  (:use plumbing.core)
  (:require
   [schema.core :as s]
   [plumbing.expiring-cache :as expiring-cache]
   [plumbing.new-time :as new-time]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schema

(s/defschema RateLimitOptions
  "Options for rate limiting a particular request."
  {:id (s/named String "The ID under which to rate limit the request")
   :security-deposit (s/named s/Num "The number of tokens deducted before the request")
   :token-limit (s/named s/Num "The maximum number of tokens accumulated")
   :token-accumulation-rate-ms (s/named s/Num "Number of tokens gained per ms.")
   :runtime->tokens-used (s/=> (s/named s/Num "Tokens used") (s/named Long "Runtime in ms"))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Private

(defn decrement-allowance
  "Takes away tokens by a given amount, will calculate how many tokens to grant as well"
  [options amount now {:keys [allowance last-check]}]
  (letk [[token-accumulation-rate-ms token-limit] options]
    (let [dejittered-now (if last-check (max last-check now) now)]
      {:last-check dejittered-now
       :allowance (if (nil? last-check)
                    (- token-limit amount)
                    (- (min token-limit
                            (+ allowance (* token-accumulation-rate-ms (- dejittered-now last-check))))
                       amount))})))

(defn should-allow?!
  "If this request should be allowed to go through. This will also deduct a security deposit"
  [rate-limit-cache options now]
  (letk [[id security-deposit] options
         [allowance] (expiring-cache/update
                      rate-limit-cache id
                      (partial decrement-allowance options security-deposit now))]
    (>= allowance 0)))

(defn deduct-tokens! [rate-limit-cache options price start-time end-time]
  "Deduct tokens at the end of a request. Return the security deposit that was taken
   at the start of the request. "
  (letk [[id security-deposit] options]
    (expiring-cache/update
     rate-limit-cache id
     (partial decrement-allowance
              options
              (- price security-deposit)
              end-time))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(defnk rate-limit-cache [{cache-expire-ms (new-time/to-millis 5 :minutes)} {cache-prune-ms 5000}]
  (expiring-cache/create-concurrent cache-expire-ms cache-prune-ms true (constantly nil)))

(defn rate-limit-middleware
  "Request->opts maps a request to rate limit options.
   On-error takes request, key, and cache entry."
  [rate-limit-cache request->options on-error handler]
  (fn [request]
    (letk [[id runtime->tokens-used :as options] (request->options request)]
      (let [start-time (millis)
            allow? (should-allow?! rate-limit-cache options start-time)]
        (try (if allow?
               (handler request)
               (on-error request id (expiring-cache/get rate-limit-cache id)))
             (finally
               (let [end-time (millis)]
                 (deduct-tokens!
                  rate-limit-cache options
                  (if allow? (runtime->tokens-used (- end-time start-time)) 0)
                  start-time end-time))))))))

(def +no-rate-limit+
  {:id "¡ultimate-power!"
   :security-deposit 0
   :token-limit 1000
   :token-accumulation-rate-ms 100
   :runtime->tokens-used (fn [_] 0)})

(defn time-rate-limit
  "Helper for rate limiting where tokens correspond to ms of server time used."
  [id security-deposit token-limit rate-ms]
  {:id id
   :security-deposit security-deposit
   :token-limit token-limit
   :token-accumulation-rate-ms rate-ms
   :runtime->tokens-used (fn [ms] ms)})

(defn call-rate-limit
  "Helper for rate limiting where tokens correspond to # of calls made.
   Allows max-calls every period seconds"
  [id token-limit period]
  {:id id
   :security-deposit 1
   :token-limit token-limit
   :token-accumulation-rate-ms (/ token-limit (* period 1000.0))
   :runtime->tokens-used (fn [ms] 1)})
