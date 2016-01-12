(ns tubes.text-metrics
  (:require
   [tubes.core :as tubes]
   [dommy.core :as dommy]))

(defn actual-text-width [el txt]
  (dommy/set-html! el txt)
  (.-offsetWidth el))

(def rel-errors (atom [0.0 0]))

(defn ^:export relative-error []
  (let [[s n] @rel-errors]
    (str (/ s n) " " n)))

(defprotocol PTextMeasurer
  (text-width [this txt]))

(defn cached-elem-text-measurer
  ([elem] (cached-elem-text-measurer actual-text-width))
  ([elem getter-fn]
     (let [text-widths (js-obj)]
       (reify PTextMeasurer
         (text-width [this txt]
           (or (aget text-widths txt)
               (let [w (getter-fn elem txt)]
                 (aset text-widths txt w)
                 w)))))))

(defn elem-text-measurer [elem]
  (reify PTextMeasurer
    (text-width [this txt]
      (actual-text-width elem txt))))

(def len-text-measurer
  (reify PTextMeasurer
    (text-width [this txt] (count txt))))

(defn approximate-bigram-width [measurer txt]
  (let [len (.-length txt)]
    (loop [width (text-width measurer (.substring txt 0 1))
           i 1]
      (if (>= i len)
        width
        (recur
         (+ width
            (- (text-width measurer (.substring txt (dec i) (inc i)))
               (text-width measurer (.substring txt (dec i) i))))
         (inc i))))))

(defn approximate-break-text
  "Returns a pair [break-idx w] where break-idx is the largest index where
  (approximate-bigram-width measurer (.substring txt 0 break-idx)) <= max-width
  and w is the width up to break-idx"
  [measurer txt max-width]
  (let [len (.-length txt)]
    (if (zero? len)
      [0 0]
      (loop [last-width 0
             next-step (text-width measurer (.substring txt 0 1))
             i 1]
        (let [width (+ last-width next-step)]
          (cond
           (> width max-width)
           [(dec i) last-width]
           (>= i len)
           [i width]
           :else
           (recur
            width
            (- (text-width measurer (.substring txt (dec i) (inc i)))
               (text-width measurer (.substring txt (dec i) i)))
            (inc i))))))))

(defn break-text [measurer txt max-width]
  (let [i
        (tubes/eggdrop-search
         #(<= (text-width measurer (.substring txt 0 %)) max-width)
         0
         (count txt))]
    [i (text-width measurer (.substring txt 0 i))]))

(defn break-text-at
  "Returns the max index of a char in break-set that satisfies
  (approximate-bigram-width measurer (.substring txt 0 break-idx)) <= max-width
  If no characters are part of break-set, returns break-text equivalent"
  [breaker txt max-width break-set]
  (let [[raw-break-idx _] (breaker txt max-width)]
    (if (= raw-break-idx (.-length txt))
      raw-break-idx
      (or (->> (range raw-break-idx 0 -1)
               (filter #(contains? break-set (.charAt txt %)))
               first)
          raw-break-idx))))

(defn split-text-at
  "Split txt at index (break-text-at ...)"
  [breaker txt max-width break-set]
  (let [break-idx (break-text-at breaker txt max-width break-set)]
    [(.substring txt 0 break-idx) (.substring txt break-idx)]))

(defn clamp-text
  "clamped 8==!===D"
  [measurer txt cell-width break-set n-lines min-hang]
  (if (< (text-width measurer txt) cell-width)
    [txt]
    (let [breaker (partial break-text measurer)
          [line more] (split-text-at breaker txt cell-width break-set)]
      (cond (> n-lines 1)
            (loop [line line more more]
              (if (and (seq line) (< (count more) min-hang))
                (let [[line word] (split-text-at #(break-text len-text-measurer %1 %2)
                                                 line (dec (count line)) break-set)]
                  ;; slightly shitty if there is no break in whole line, or if
                  ;; we already broke without a break.
                  (recur line (str word " " more)))
                (cons line (clamp-text
                            measurer (.trim more) cell-width break-set (dec n-lines)
                            min-hang))))

            (seq more)
            (let [ellipsis "…"
                  ellipsis-width (text-width measurer ellipsis)
                  [line more] (split-text-at
                               breaker txt (- cell-width ellipsis-width) break-set)]
              (list (str (.trim line) ellipsis)))

            :else (list line)))))
