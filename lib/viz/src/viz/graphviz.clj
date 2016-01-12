(ns viz.graphviz
  (:use plumbing.core)
  (:require
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [viz.pdf :as pdf])
  (:import [java.util HashSet] [java.io File]))

;; Crap copied from my old utils
(defn double-quote [s] (str "\"" s "\""))

(defn file-stem [^String path]
  (let [i (.lastIndexOf path ".")]
    (if (>= i 0)
      (.substring path 0 i)
      path)))

(defn fresh-random-filename
  ([prefix] (fresh-random-filename prefix ""))
  ([prefix suffix]
     (first
      (for [i (repeatedly #(rand-int 10000000))
            :let [fn (str prefix (when-not (= i 1) i) suffix)]
            :when (not (.exists (File. fn)))]
        fn))))


                                        ; nodes is a set of nodes, edges is a map from sets of edges to costs
(def ^:dynamic *default-graphviz-dir* "/tmp/")


(defn- attribute-string [label-or-attribute-map]
  (when label-or-attribute-map
    (str "["
         (str/join ","
                   (map (fn [[k v]] (str (name k) "=" v))
                        (if (map? label-or-attribute-map)
                          label-or-attribute-map
                          {:label (double-quote label-or-attribute-map)})))
         "]")))



(defn- walk-graph [root node-key-fn node-label-fn edge-child-pair-fn ^HashSet visited indexer]
  (let [node-key  (node-key-fn root)
        node-map (node-label-fn root)]
                                        ;    (println node-key)
                                        ;    (println node-name)
                                        ;    (println node-map)
    (when-not (.contains visited node-key)
      (.add visited node-key)
      (apply str
             (indexer node-key) (attribute-string node-map) ";\n"
             (apply concat
                    (for [[edge-map child] (edge-child-pair-fn root)]
                      (cons (str (indexer node-key) " -> " (indexer (node-key-fn child))
                                 (attribute-string edge-map)
                                 ";\n")
                            (walk-graph child node-key-fn node-label-fn edge-child-pair-fn visited indexer))))))))

(defn- dot-file-contents [roots node-key-fn node-label-fn edge-child-pair-fn]
  (let [indexer (memoize (fn [x] (double-quote (gensym))))
        vis      (HashSet.)]
    (str "strict digraph {\n"
         " rankdir = LR;\n"
                                        ;             " rotate=90;\n"
         (apply str (for [root roots] (walk-graph root node-key-fn node-label-fn edge-child-pair-fn vis indexer)))
         "}\n")))

(defn dot-file-contents-el [el & [nl]]
  (let [nl (or nl {})
        em (map-vals #(map second %) (group-by first el))]
    (dot-file-contents
     (set (concat (keys nl) (apply concat el)))
     identity #(get nl % %) #(for [e (get em %)] [nil e]))))

(defn write-graphviz
  ([roots node-key-fn node-label-fn edge-child-pair-fn]
     (write-graphviz
      (fresh-random-filename *default-graphviz-dir* ".dot")
      roots node-key-fn node-label-fn edge-child-pair-fn))
  ([filename roots node-key-fn node-label-fn edge-child-pair-fn]
     (let [pdf-file (str (file-stem filename) ".pdf")]
       (println filename)
       (spit filename (dot-file-contents roots node-key-fn node-label-fn edge-child-pair-fn)
             )
       (shell/sh "dot" "-Tpdf" "-o" pdf-file filename)
       pdf-file)))

(defn graphviz
  ([root node-key-fn node-label-fn edge-child-pair-fn]
     (pdf/show-pdf-page (doto (write-graphviz [root] node-key-fn node-label-fn edge-child-pair-fn) prn)))
  ([filename root node-key-fn node-label-fn edge-child-pair-fn]
     (pdf/show-pdf-page (doto (write-graphviz filename [root] node-key-fn node-label-fn edge-child-pair-fn) prn))))

(defn graphviz-el
  "Draw a graph given an edge list, and optional node label map."
  ([el] (graphviz-el el {}))
  ([el nl]
     (let [em (map-vals #(map second %) (group-by first el))]
       (pdf/show-pdf-page
        (doto
            (write-graphviz
             (set (concat (keys nl) (apply concat el)))
                                        ;          (or (seq (clojure.set/difference (set (concat (keys nl) (apply concat el))) (set (map second el))))
                                        ;              [(first (first el))])
             identity #(get nl % %) #(for [e (get em %)] [nil e]))
          prn)))))

                                        ; (graphviz 0 identity str (fn [i] (into {} (for [j (range (inc i) (min 10 (+ i 3)))] [j j]))))
;; (gv/graphviz-el (for [[k v] g i (keys (plumbing.fnk.pfnk/input-schema v))] [i k]) (map-from-keys identity (concat (keys g) (mapcat (comp keys plumbing.fnk.pfnk/input-schema) (vals g)))))

                                        ; http://www.graphviz.org/Documentation.php
                                        ; http://www.graphviz.org/doc/info/lang.html
                                        ; http://www.graphviz.org/doc/info/attrs.html#dd:orientation

                                        ; http://www.dynagraph.org/documents/dynagraph.html
