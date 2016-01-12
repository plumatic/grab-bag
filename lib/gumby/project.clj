(defproject gumby "0.0.1-SNAPSHOT"
  :description "A better Elasticsearch client"
  :internal-dependencies [plumbing store]
  :external-dependencies [org.elasticsearch/elasticsearch
                          org.elasticsearch/elasticsearch-cloud-aws
                          org.clojure/core.async]
  :jvm-opts ["-server" "-mx8g" "-XX:+UseConcMarkSweepGC" "-XX:+CMSClassUnloadingEnabled" "-XX:MaxPermSize=512M"])
