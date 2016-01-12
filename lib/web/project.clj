(defproject web "0.0.5-SNAPSHOT"
  :internal-dependencies [plumbing html-parse store]
  :external-dependencies [org.apache.httpcomponents/httpclient
                          io.netty/netty
                          prismatic/fnhouse
                          org.apache.httpcomponents/httpcore
                          commons-codec
                          commons-io
                          ring/ring-core
                          ring/ring-jetty-adapter]
  :java-source-paths ["jvm"])
