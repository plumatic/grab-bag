(defproject kinesis "0.1.0-SNAPSHOT"
  :description "A Clojure wrapper for the AWS kinesis client library, and helpers for publishing."
  :internal-dependencies [plumbing aws store crane]
  :dependencies [[com.amazonaws/amazon-kinesis-client "1.2.0"]])
