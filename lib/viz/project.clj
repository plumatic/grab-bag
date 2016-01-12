(defproject viz "0.0.1-SNAPSHOT"
  :internal-dependencies [plumbing]
  :jvm-opts ["-mx1200m"]
  :cljs-externs ["resources/d3.v2.js"]
  :cljs-output-dir "resources/out"
  :cljs-output-to "resources/main.js"
  :cljs-optimizations :simple)
