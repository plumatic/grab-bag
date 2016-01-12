(ns viz.test)

(def world
  {:size [1.0 1.0]
   :gravity 10.0
   :dt 1.0e-3
;   :pen-fn #(if (> % 0) (- (Math/exp (* 50 %)) 1) 0)
   :pen-fn #(if (> % 0) (* 1000 %) 0)
   :size-kp 10000.0
   :size-decay 0.97
   :friction  0.98
   :sleep 5
   })

(declare to-js)

(defn jsArr
  "Recursively converts a sequential object into a JavaScript array"
  [seq]
  (let [a (array)]
    (doseq [x seq] (.push a (to-js x)))
    a))

(defn jsObj
  "Convert a clojure map into a JavaScript object"
  [obj]
  (let [o (js-obj)]
    (doseq [[k v] obj]
      (let [k (if (keyword? k) (name k) k)
	    v (if (keyword? v) (name v) v)]
	(aset o k (to-js v))))
    o))

(defn to-js [x]
  (cond 
   (map? x) (jsObj x)
   (sequential? x) (jsArr x)
   (keyword? x) (name x)
   :else x))

(defn pr [x] (.log js/console  (js/JSON.stringify  x)))

(defn map-from-vals [f xs]
  (into {} (map (fn [x] [(f x) x]) xs)))

(defn d3-this []
  (.select js/d3 (js* "this")))

(defn draw-circles [[svg to-x to-y] circles]
  (when (seq circles)

    (let [
	  cs (-> svg
		 (.selectAll "circle")
		 (.data (to-js (take 30 circles))))]
      ;; Create New Circles
      (-> cs
	  .enter
	  (.append "svg:circle")
	  (.on "mouseover" (fn []
			     (-> (.select js/d3 (js* "this"))
				 (.attr "fill" "steelblue"))))) 
      ;; Kill Old
      (-> cs .exit .remove)
      ;; Set Attributes for all circles
      (-> cs
	  (.attr "cx"
		 (fn [c] (-> c (. -center) first to-x)))
	  (.attr "cy"
		 (fn [c] (-> c (. -center) second to-y)))
	  (.attr "r"
		 (fn [c] (-> c (. -size) (. -radius) to-x)))
	  (.attr "fill" (fn [c] (. c -color)))))))

(defn clip [world pos r]
  (let [;eps 0.0001
        r2 (/ r 2)
        [w h] (:size world)
        [x y] pos]
    [(max r2 (min (- w r2) x))
     (max r2 (min (- h r2) y))]))

(defn v-step [world circles]
  (let [{:keys [dt]} world]
    (for [{:keys [center vel size] :as c} circles]
      (assoc c
        :center (clip world (map #(+ %1 (* dt %2)) center vel) (-> c :size :radius))
        :size (let [{:keys [volume dv]} size
                    nv (+ volume (* dt dv))]
                (assoc size
                  :volume nv
                  :radius (.sqrt js/Math nv)))))))

(defn wall-force [dim v dv pen-fn]
  (fn [c]
    (assoc [0 0] dim
	   (* -1 dv
	      (pen-fn (* dv (- (+ (nth (:center c) dim)
				  (* dv (-> c :size :radius)))
			       v)))))))

(defn norm [[x y]]
  (.sqrt js/Math (+ (* x x) (* y y))))

(defn circle-force [pen-fn circle]
  (let [center (:center circle)
	radius (-> circle :size :radius)]
    (fn [c]
      (let [dp (map - center (:center c))
	    dist (norm dp)
	    rs (+ radius (-> c :size :radius))]
	(if (or (= dist 0.0) (> dist rs))
	  [0 0]
	  (let [f (pen-fn (- rs dist))]
	    (map #(* -1 (/ % dist) f ) dp)))))))


(defn a-step [world circles]
  (let [{:keys [dt gravity pen-fn size-kp size-decay friction]} world
        [width height] (:size world)
        wall-forces  [(wall-force 0 0 -1 pen-fn)
                      (wall-force 1 0 -1 pen-fn)
                      (wall-force 0 width 1 pen-fn)
                      (wall-force 1 height 1 pen-fn)]
        forces (concat wall-forces (map (partial circle-force pen-fn) circles))]    
    (for [{:keys [vel size] :as c} circles]
      (let [m (:volume size)
            f (apply map + [0 (* -1 gravity m)] (map #(% c) forces))]
       (assoc c
         :vel (map #(+ (* friction %1) (* dt (/ %2 m))) vel f) 
         :size (let [{:keys [target volume]} size]
                 (assoc size :dv
                        (+ (* size-decay (:dv size))
                           (* dt size-kp (- target volume))))))))))

(defn rand-selection [xs]
  (let [r (.random js/Math)
	n (count xs)
	i (.round js/Math (* n r))]
    (nth xs i)))

(defn circle [pos vol target label]
  {:center pos
   :vel [0 0]
   :size {:volume vol
          :radius (.sqrt js/Math vol)
          :target (or target vol)
          :dv 0}
   :color (rand-selection ["orange" "blue" "red" "green" "grey"])
   :label (or label (str pos))})

(defn update-circles [circles]
  (let [ir 0.03, iv (* ir ir)
	make-rand #(let [r (.random js/Math)]
		     (.round js/Math (* r  r  r 50)))
	new (->> (repeatedly make-rand)
		 distinct
		 (take 3))]
    (->> new
        (reduce
         (fn [idx i]
           (let [i (str i)]
             (assoc idx i 
                    (if-let [c (get idx i)]
                      (update-in c [:size :target] + iv)
                      (circle
		       [(+ ir (* (.random js/Math) (- 1 (* 2 ir))))
			(- ir 0.001)]
		       iv iv i)))))
         (map-from-vals :label circles))
        vals)))


(defn animate [world circles n-steps]
  (draw circles)
  (when (> n-steps 0)
    (js/setTimeout
      #(animate world (->> circles (v-step world) (a-step world)) (dec n-steps))
      (:sleep world))))

(defn animate-add
  ([world draw n-steps]
     (animate-add world draw n-steps nil))
  ([world draw n-steps circles]
     (when (zero? (mod n-steps 2))
       (draw-circles draw circles))
     (if (> n-steps 0)
       (js/setTimeout
	#(let [post-sim (->> circles (v-step world) (a-step world))]
             (animate-add world draw (dec n-steps)
              (if (zero? (mod n-steps 10))
		(update-circles post-sim)
		post-sim)))
	10)
       circles)))

(defn animate-settle [world n-steps]
  (animate world (animate-add world n-steps) n-steps))

(defn draw-data []
  (let [w 300
	h 300
	svg (-> js/d3
	     (.select "#demo")
	     (.append "svg:svg")
	     (.attr "width" w)
	     (.attr "height" h))
	to-x (-> (.linear js/d3.scale)
	      (.domain (to-js [0 1.0]))
	      (.range (to-js [0 w])))
	to-y (-> (.linear js/d3.scale)
	      (.domain (to-js [0 1.0]))
	      (.range (to-js [h 0.0])))]
    [svg to-x to-y]))

(defn ^:export animate []
  (animate-add world (draw-data) 1000))