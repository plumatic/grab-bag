(ns web.async-client-test
  (:use clojure.test plumbing.test)
  (:require
   [plumbing.resource :as resource]
   [web.async-client :as async-client]))

(deftest ^:flaky async-resolve-smoke-test
  (resource/with-open [c (async-client/async-http-client-resource {})]
    (doseq [[url resolved]
            [
             ["http://t.co/2qvQHoDEPx"
              "https://publish.awswebcasts.com/content/connect/c1/7/en/events/event/shared/4267284/event_landing.html?campaign-id=PW&sco-id=4788418"]

             ["http://hubs.ly/y0hLSH0"
              "http://www.wired.com/2014/11/on-david-marcus-and-facebook/"]

             ["http://hubs.ly/y0jBVz0"
              "https://stream.disruption.vc/notes/4232"]]]
      (let [o (atom nil)]
        (async-client/safe-canonical-url c url #(reset! o %) (constantly true))
        (is-eventually (= @o resolved))))))
