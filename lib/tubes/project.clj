(defproject grabbag/tubes "0.0.1-SNAPSHOT"
  :description "Util funcs for CLJS"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :clojurescript? true
  :internal-dependencies [plumbing]
  :external-dependencies [org.clojure/clojurescript
                          prismatic/cljs-test
                          prismatic/dommy]
  :pom-addition [:developers [:developer
                              [:name "Grabbag"]
                              [:url "http://example.com"]
                              [:email "admin+oss@example.com"]
                              [:timezone "-8"]]]
  :plugins [[lein-cljsbuild "0.3.2"]]
  :hooks [leiningen.cljsbuild]
  :cljsbuild
  {:builds
   {:dev {:source-paths ["src"]
          :compiler {:output-to "target/main.js"
                     :optimizations :whitespace
                     :pretty-print true}}
    :test {:source-paths ["test"]
           :incremental false
           :compiler {:output-to "target/unit-test.js"
                      :optimizations :whitespace
                      :pretty-print true}}}
   :test-commands {"unit" ["phantomjs" "target/unit-test.js" "resources/test.html"]}})
