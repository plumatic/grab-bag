(defproject domain "0.0.1-SNAPSHOT"
  :description "Domain objects for Grabbag (Docs, Users, ...)"
  :internal-dependencies [plumbing flop store web]
  :external-dependencies [trove/trove com.twitter/twitter-text
                          prismatic/hiphip])
