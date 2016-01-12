(defproject example-job "0.0.1-SNAPSHOT"
  :description "Example job to show clojure-hadoop/haderp"
  :internal-dependencies [plumbing store haderp]
  ;; lein-repo isn't able to propagate profile info between projects,
  ;; (seems hard), or :main (would be easy to add), so every job should include
  ;; the following two sections:
  :profiles {:provided {:dependencies
                        [[org.apache.hadoop/hadoop-common "2.6.0"]
                         [org.apache.hadoop/hadoop-mapreduce-client-core "2.6.0"]
                         [org.apache.hadoop/hadoop-aws "2.6.0"]
                         ;; to run locally
                         [org.apache.hadoop/hadoop-mapreduce-client-jobclient "2.6.0"]]}}

  :main nil
  ;; allow to run in local hadoop in standalone (between in-process and remote EMR)
  ;; (works around IOException: Mkdirs failed to create /var/folders/_g/_nhxhpvn249c7ylfjwftblvr0000gn/T/hadoop-unjar1744564428866830048/license)
  :uberjar-exclusions [#".*LICENSE"]
  )
