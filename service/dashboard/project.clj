(defproject dashboard "0.0.1-SNAPSHOT"
  :service? true
  :plugins [[lein-cljsbuild "0.3.2"]]
  :external-dependencies [org.clojure/clojurescript
                          org.clojure/tools.nrepl
                          org.clojure/core.async
                          prismatic/cljs-test
                          prismatic/dommy
                          prismatic/fnhouse
                          prismatic/om-tools
                          hiccup
                          reagent
                          garden
                          om
                          crate
                          cljs-http

                          ;; for reaching into DWS
                          c3p0/c3p0
                          org.clojure/java.jdbc
                          mysql/mysql-connector-java

                          ;; For accessing google analytics
                          com.google.api-client/google-api-client
                          com.google.apis/google-api-services-analytics
                          com.google.api-client/google-api-client-jackson2
                          ]
  :internal-dependencies [plumbing web crane service store
                          tubes cljs-request]
  :jvm-opts ["-server" "-Xmx2g" "-XX:+UseConcMarkSweepGC" "-XX:+CMSClassUnloadingEnabled" "-XX:MaxPermSize=512M"]
  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :pre-deploy-cmds [["lein" "cljsbuild" "once"]]
  :cljsbuild
  {:test-commands {"unit" ["phantomjs" "resources/dashboard/target/js/tests.js"]}
   :builds
   {:dev
    {:source-paths ["src/cljs"]
     :compiler
     {:output-to "resources/dashboard/target/js/dashboard.js"
      :pretty-print true
      :preamble ["react/react.js" "d3/d3.min.js"]
      :externs ["react/externs/react.js"]}}
    :test
    {:source-paths ["src/cljs" "test"]
     :incremental false
     :compiler
     {:output-to "resources/dashboard/target/js/tests.js"}}}})
