(defproject classify "0.0.1-SNAPSHOT"
  :description "FIXME: write"
  :internal-dependencies [plumbing flop web]
  :external-dependencies [prismatic/hiphip]
  :java-source-paths ["src/jvm"]
  :jvm-opts ["-server" "-Dfile.encoding=UTF8" "-Xmx6g"])
