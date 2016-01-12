(defproject service "0.0.1-SNAPSHOT"
  :description "primary service functionality"
  :internal-dependencies [aws plumbing store web crane viz email]
  :external-dependencies [org.clojure/tools.nrepl metrics-clojure riemann-clojure-client])
