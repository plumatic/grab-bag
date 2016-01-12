(defproject haderp "0.0.1-SNAPSHOT"
  :description "Library for hadoop / EMR jobs"
  :internal-dependencies [plumbing web crane store]
  :external-dependencies [com.amazonaws/aws-java-sdk]
  :dependencies [[w01fe/clojure-hadoop "1.4.7-SNAPSHOT"]]
  :main nil)
