(ns service.remote-repl
  (:use plumbing.core))

;; TODO: replace this with swank?
(def resources (atom nil))
