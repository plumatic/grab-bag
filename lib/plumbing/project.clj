(defproject plumbing "0.1.5-SNAPSHOT"
  :internal-dependencies []
  :external-dependencies [com.googlecode.concurrentlinkedhashmap/concurrentlinkedhashmap-lru
                          com.stuartsierra/lazytest
                          commons-codec
                          commons-io
                          joda-time
                          log4j/log4j
                          org.clojure/java.classpath
                          org.clojure/test.check
                          org.clojure/tools.namespace
                          com.fasterxml.jackson.core/jackson-core
                          org.xerial.snappy/snappy-java
                          potemkin
                          prismatic/plumbing
                          prismatic/schema]
  :java-source-paths ["jvm"]
  :repositories {"apache" "https://repository.apache.org/content/repositories/releases/"
                 "stuartsierra-releases" "http://stuartsierra.com/maven2"
                 "stuartsierra-snapshots" "http://stuartsierra.com/m2snapshots"})
