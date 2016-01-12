(ns viz.chart
  "Prereqs:
    brew install gnuplot
    brew install imagemagick --with-x11

   Example:
    (viz.chart/single {:series [[1 2] [3 4] [5 6]]})

   currently imagemagick display seems to have some issues on mac os x, so
   we might want to default to png / java pdf display for local exploration."
  (:use plumbing.core)
  (:require
   [clojure.java.shell :as shell]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [plumbing.core-incubator :as pci]
   [plumbing.math :as math]
   [viz.pdf :as pdf])
  (:import [java.io File]))

(set! *warn-on-reflection* true)

(defn fresh-random-filename
  ([prefix] (fresh-random-filename prefix ""))
  ([prefix suffix]
     (first
      (for [i (repeatedly #(rand-int 10000000))
            :let [fn (str prefix (when-not (= i 1) i) suffix)]
            :when (not (.exists (File. fn)))]
        fn))))

(defn single-quote [x] (str "'" x "'"))
(defn double-quote [x] (str "\"" x "\"" ))

(def ^:dynamic *default-gnuplot-dir* "/tmp/")

(def +series-ks+
  #{:title :data :categorical
    :type :lw :lt :lc :ps :pt})


(def +chart-ks+
  #{:size :pointsize
    :xrange :xtics :mxtics :xlog :xlabel
    :yrange :ytics :mytics :ylog :ylabel
    :key :title :term :extra-commands
    :default-series-options :series})

(def +default-series-options+
  {:type "linespoints"
   :lw   2
   :lt  (cycle [1 2 3 4 5 6 7])
   :pt  (cycle [1 1 1 1 1 1 1 2 2 2 2 2 2 2 3 4 5 6 7])})

(def +default-chart-options+
  {:pointsize 1
   :key       "off"
   :default-series-options +default-series-options+})



(defn fn-or [& args]
  (some identity (reverse args)))

(defn dump-series
  ([series] (dump-series series (fresh-random-filename *default-gnuplot-dir* ".tmpgd")))
  ([series filename]
     (assert (every? +series-ks+ (keys series)))
     (spit filename
           (str/join "\n"
                     (map (fn [dp] (str/join ", " (map #(if (number? %) (double %) %) dp)))
                          (safe-get series :data))))
     (apply str (single-quote filename)
            " using "
            (case (count (first (:data series)))
              1 "0:1"
              2 "1:2"
              3 (if (-> series :type keyword #{:boxerror}) "1:2:3" "1:2:xticlabels(3)")
              4 "1:2:3:4")
            (when (:title series) (str " t " (double-quote (:title series))))
            (when (:type series)  (str " with " (:type series)))
            (for [field ["lt" "lw" "lc" "ps" "pt"] :when  ((keyword field) series)]
              (str " " field " " ((keyword field) series))))))

(defn peel-series [m]
  [(map-vals #(if (coll? %) (first %) %) m)
   (map-vals #(if (coll? %) (next %) %) m)])

(defn drain [^java.io.InputStream i]
  (while (> (.available i) 0) (.read i)))

(defn gnuplot-process [] (let [p (.exec (Runtime/getRuntime) "gnuplot")]
                           {:process p
                            :out (java.io.PrintWriter. (.getOutputStream p))
                            :drain (fn [] (drain (.getInputStream p)) (drain (.getErrorStream p)))}))


(defonce gnuplot (atom (delay (gnuplot-process))))

(defn reset-gnuplot! []
  (let [^Process p (:process @@gnuplot)]
    (try (.destroy p) (catch Throwable t))
    (reset! gnuplot (delay (gnuplot-process)))))

(defn plot
  ([chart] (plot chart :magick))
  ([chart out-file] (plot chart out-file false (fresh-random-filename *default-gnuplot-dir* ".tmpgp")))
  ([chart out-file show? filename]
     (assert (not (= out-file :bytes)))
     (let [chart                  (merge +default-chart-options+ chart)
           default-series-options (:default-series-options chart)
           {:keys [drain ^java.io.PrintWriter out]} @@gnuplot]
       (locking out
         (assert (every? +chart-ks+ (keys chart)))
         (spit filename
               (str
                (str
                 "set term "
                 (cond (or (#{:bytes :magick} out-file)
                           (.endsWith ^String out-file "png"))
                       "png"

                       (.endsWith ^String out-file "pdf")
                       "pdf enhanced dashed color"

                       (.endsWith ^String out-file "eps")
                       "postscript eps enhanced clip dashed color"

                       :else (throw (RuntimeException. "unknown output type")))
                 " " (:term chart) "\n" )
                (case out-file
                  :magick "set output '| display png:-'\n"
                  :bytes ""
                  (str "set output " (single-quote out-file) "\n"))

                                        ;"set term size 100, 100\n"

                "set autoscale x \n"
                "set autoscale y \n"
                (apply str
                       (for [field ["pointsize" "size" "key"
                                    "xrange" "xtics" "mxtics"
                                    "yrange" "ytics" "mytics"]
                             :when ((keyword field) chart)]
                         (str "set " field " " ((keyword field) chart) "\n")))
                (apply str
                       (for [field ["title" "xlabel" "ylabel"]
                             :when ((keyword field) chart)]
                         (str "set " field " " (double-quote ((keyword field) chart)) "\n")))
                (if (:xlog chart) "set logscale x\n" "unset logscale x\n")
                (if (:ylog chart) "set logscale y\n" "unset logscale y\n")


                (apply str (for [c (:extra-commands chart)] (str c "\n")))
                "plot "
                (str/join ", \\\n"
                          (loop [in (seq (:series chart)), out [], dso default-series-options]
                            (if in
                              (let [[cso next-dso] (peel-series dso)]
                                (recur
                                 (next in)
                                 (conj out (dump-series (merge-with fn-or cso (first in))))
                                 next-dso))
                              out)))
                "\n"
                )))
       (let [o nil #_
             (apply shell/sh "gnuplot" filename (when (= out-file :bytes) [:out-enc :bytes]))]
         (.print out (str "load " (double-quote filename) "\n"))
         (.flush out)
         (drain)
         (when show? (shell/sh "open" out-file))
         (if (= out-file :bytes)
           (:out o)
           out-file)))))


(defn gp-rgb [r g b]
  (doseq [x [r g b]] (assert (and (>= x 0) (< x 256))))
  (format "rgbcolor \"#%02x%02x%02x\"" (int r) (int g) (int b)))






;;; Dataset and table stuff

;; Data set is just a list of maps with the same keys.

(defn ds-derive [f ds new-field]
  (for [x ds]
    (assoc x new-field (f x))))

(defn ds-summarize [ds group-fields field-combiner-extractors]
  (for [[k v] (group-by (fn [d] (vec (map #(get d %) group-fields))) ds)]
    (into {}
          (concat
           (map vector group-fields k)
           (for [[field combiner extractor] field-combiner-extractors]
             [field (apply combiner (map extractor v))])))))


(defn to-series [group-fn x-fn y-fn data & [sort-fn]]
  ((or sort-fn (partial sort-by key))
   (map-vals
    (fn [series] (map (juxt x-fn y-fn) series))
    (group-by group-fn data))))

(defnk multi [series {series-options {}} {chart-options {:key "top left"}} {categorical nil} {out-file :magick} :as m]
  (doseq [s series] (assert (= 2 (count s))))
  (let [categorical (if (contains? m :categorical-sort)
                      (:categorical-sort m)
                      (when (and (not (number? (first (ffirst (vals series)))))
                                 (not (= (count (ffirst (vals series))) 3)))
                        (sort (distinct (map first (apply concat (vals series)))))))
        cat-idx     (when categorical (for-map [[i k] (indexed categorical)] k i))
        c (assoc chart-options
            :series
            (for [[k vs] (sort-by first series)]
              (assoc (series-options k)
                :categorical categorical
                :title k
                :data (sort-by
                       first
                       (if categorical (map (fn [[x y]] [(safe-get cat-idx x) y x]) vs) vs)))))]
    (if out-file
      (plot c out-file)
      c)))

(defnk single [series {chart-options {}} :as m]
  (multi (assoc m :series {:single series})))


(defn histogram
  "Plot a histogram of values.  Bin-spec can be nil (auto), a number,
   a sequence of bin values, or later a map describing other options."
  [bin-spec values & [multi-args]]
  (let [values (sort values)
        bins (or bin-spec
                 (max 10 (min 100 (quot (count values) 10))))
        bins (vec
              (if (number? bins)
                (->> values
                     (pci/partition-evenly bins)
                     (map first)
                     next)
                bins))
        d (math/fast-discretizer bins)
        series (->> values
                    (map d)
                    frequencies-fast
                    (sort-by key)
                    (map (fn [b [_ v]] [b v]) bins))]
    (multi (assoc multi-args :series {:count series}))))


(set! *warn-on-reflection* false)



(comment
  (defn postprocess-pdf [f]
    (when show?
      (show-pdf-page pdf-file))
    (shell/sh "pdfcrop" pdf-file pdf-file)
    pdf-file))

(comment
  (defn postprocess-eps [eps-file]
    (let [stem (fresh-random-filename *default-gnuplot-dir* )
          eps-file (str stem ".eps")]
      (println (shell/sh "gnuplot" (doto (dump-chart chart eps-file) prn)))
                                        ;        (println eps-file pdf-file)
                                        ;        (println (shell/sh "epstopdf" #_ (str "--outfile=\"" pdf-file "\"") eps-file))
                                        ;        (shell/sh "cp" (str stem ".pdf") pdf-file)
                                        ;        (when show? (pdf/show-pdf-page pdf-file))
                                        ;        pdf-file
      (shell/sh "open" eps-file)
      eps-file
      )))
