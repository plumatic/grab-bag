(defproject model-explorer "0.0.1-SNAPSHOT"
  :description "A generic service to explore training data and train models."
  :internal-dependencies [plumbing store web domain service classify crane]
  :external-dependencies [garden prismatic/fnhouse]
  :service? true
  :jvm-opts ["-server" "-mx8000m" "-Djava.awt.headless=true" "-Dfile.encoding=UTF8"
             "-XX:+UseConcMarkSweepGC" "-XX:+CMSClassUnloadingEnabled" "-XX:MaxPermSize=512M"])
