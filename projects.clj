{required-dependencies
 [[clj-stacktrace "0.2.5"]
  ^:force [org.clojure/clojure "1.7.0"]
  [org.clojure/tools.logging "0.2.0"]]

 external-dependencies
 [[c3p0/c3p0 "0.9.1.2"]
  [clj-robots "0.6.0"]
  [clj-ssh "0.5.9"]
  [clj-tagsoup "0.3.0"]
  [clj-webdriver "0.6.0"]
  [cljs-http "0.1.10"]
  [com.andrewmcveigh/cljs-time "0.3.10"]
  [com.amazonaws/aws-java-sdk "1.9.34" :exclusions [joda-time]]
  ^:force [com.cemerick/clojurescript.test "0.3.0"]
  [com.drewnoakes/metadata-extractor "2.8.0"]
  [com.googlecode.concurrentlinkedhashmap/concurrentlinkedhashmap-lru "1.3.1"]
  [com.googlecode.owasp-java-html-sanitizer/owasp-java-html-sanitizer "r223"]
  [com.google.api-client/google-api-client "1.18.0-rc"]
  [com.google.apis/google-api-services-analytics "v3-rev94-1.18.0-rc"]
  [com.google.api-client/google-api-client-jackson2 "1.18.0-rc"]
  [com.h2database/h2 "1.4.187"]
  [com.joestelmach/natty "0.11"]
  ^:force [com.keminglabs/cljx "0.6.0" :exclusions [org.clojure/clojure]]
  [com.keminglabs/jzmq "a6c1706"]
  [com.keminglabs/jzmq-linux64 "a6c1706"]
  [com.keminglabs/jzmq-osx64 "a6c1706"]
  [com.keminglabs/zmq-async "0.1.0"]
  [com.maxmind.geoip/geoip-api "1.2.11"]
  [com.notnoop.apns/apns "0.2.3"]
  [com.sleepycat/je "5.0.34"]
  [com.stuartsierra/lazytest "1.1.2" :exclusions [swank-clojure org.clojure/clojure-contrib]]
  [com.twitter/twitter-text "1.10.2"]
  [commons-codec "1.5"]
  [commons-io "2.0.1"]
  [commons-lang "2.3"]
  [edu.stanford.nlp/stanford-corenlp "1.3.4"]
  [garden "1.1.5" :exclusions [org.clojure/clojure com.keminglabs/cljx org.clojure/clojurescript]]
  [shodan "0.4.1"]
  [hiccup "1.0.2"]
  [honeysql "0.6.1"]
  [inflections "0.9.5"]
  [io.netty/netty "3.5.7.Final"]
  [it.sauronsoftware.cron4j/cron4j "2.2.5"]
  [javax.mail/mail "1.4.4"]
  [joda-time "2.4"]
  [log4j/log4j "1.2.16"]
  [metrics-clojure "2.3.0"]
  [myguidingstar/clansi "1.3.0"]
  [mysql/mysql-connector-java  "5.1.25"]
  [om "0.7.3"] ;; NOTE: uses React 0.11.1. If upgrading om, ensure webapp uses correct version
  [crate "0.2.5"]
  [org.apache.hadoop/hadoop-common "2.6.0" :exclusions [com.jcraft/jsch]]
  [org.apache.httpcomponents/httpclient "4.2.2"]
  [org.apache.httpcomponents/httpcore "4.2.2"]
  [org.apache.lucene/lucene-core "4.10.4"]
  [org.apache.lucene/lucene-analyzers-common "4.10.4"]
  [org.clojars.pjt/opennlp-tools "1.4.3"]
  [org.clojure/core.async "0.1.346.0-17112a-alpha"]
  [org.clojure/data.csv "0.1.2"]
  ^:force [org.clojure/clojurescript "0.0-3196"]
  [org.clojure/data.zip "0.1.0"]
  [org.clojure/java.classpath "0.1.0"]
  [org.clojure/java.jdbc "0.3.2"]
  [org.clojure/test.check "0.8.0"]
  [org.clojure/tools.namespace "0.2.4"]
  ^:force [org.clojure/tools.nrepl "0.2.10"]
  [com.fasterxml.jackson.core/jackson-core "2.3.2"]
  [org.elasticsearch/elasticsearch "1.5.2"]
  [org.elasticsearch/elasticsearch-cloud-aws "2.5.1"]
  [org.imgscalr/imgscalr-lib "4.2"]
  [org.mindrot/jbcrypt "0.3m"]
  ;; Note(manish. 3/13/2015): snappy 1.1.1.6 uses a lot more memory than 1.1.1
  [org.xerial.snappy/snappy-java "1.1.1"]
  [org.zeromq/cljzmq "0.1.4" :exclusions [org.zeromq/jzmq]]
  [potemkin "0.3.2"]
  [prismatic/cljs-test "0.0.6"]
  [prismatic/dommy "1.0.0"]
  [prismatic/fnhouse "0.1.2"]
  [prismatic/hiphip "0.2.1"]
  [prismatic/om-tools "0.3.11"]
  ^:force [prismatic/plumbing "0.4.3"]
  ^:force [prismatic/schema "1.0.3"]
  [quiescent "0.2.0-RC2"]
  [reagent "0.4.1"]
  [secretary "1.2.3"]
  [sablono "0.3.4" :exclusions [cljsjs/react]]
  [riemann-clojure-client "0.2.9"]
  [ring/ring-core "1.0.0-RC1"]
  [ring/ring-jetty-adapter "1.0.0-RC1"]
  [trove/trove "2.1.1"]
  [xalan "2.7.1"]
  [xerces/xercesImpl "2.9.1"]
  [postgres-redshift "8.4-703.jdbc4"]]

 internal-dependencies
 {aws "lib/aws"
  classify "lib/classify"
  crane "lib/crane"
  domain "lib/domain"
  email "lib/email"
  flop "lib/flop"
  gumby "lib/gumby"
  haderp "lib/haderp"
  hadoop-wrapper "lib/hadoop-wrapper"
  html-parse "lib/html-parse"
  kinesis "lib/kinesis"
  plumbing "lib/plumbing"
  service "lib/service"
  store "lib/store"
  viz "lib/viz"
  web "lib/web"

  dashboard "service/dashboard"
  model-explorer "service/model-explorer"
  monitoring "service/monitoring"
  test-service "service/test-service"


  ;; clojurescript
  cljs-request "lib/cljs-request"
  tubes "lib/tubes"}}
