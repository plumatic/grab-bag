(ns model-explorer.templates-test
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [model-explorer.templates :as templates]))

(deftest interpolate-js-test
  (is-= "JSON.stringify({\"a\":1 + 1,\"c\":5})"
        (templates/interpolate-js {:a "$1" "$2" 5} {"$1" "1 + 1" "$2" "\"c\""})))
