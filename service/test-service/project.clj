(defproject test-service "0.0.1-SNAPSHOT"
  :internal-dependencies [plumbing store web crane service]
  :service? true
  :jvm-opts ["-server" "-mx1500m" #_ "-Djava.awt.headless=true" "-Dfile.encoding=UTF8"])
