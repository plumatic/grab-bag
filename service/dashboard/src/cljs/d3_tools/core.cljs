(ns d3-tools.core)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Interpolate

(defn interpolate [type]
  (case type
    :array (.-interpolateArray js/d3)
    :hsl (.-interpolateHsl js/d3)
    :hcl (.-interpolateHcl js/d3)
    :lab (.-interpolateLab js/d3)
    :number (.-interpolateNumber js/d3)
    :rgb (.-interpolateRgb js/d3)
    :object (.-interpolateObject js/d3)
    :round (.-interpolateRound js/d3)
    :string (.-interpolateString js/d3)
    :transform (.-interpolateTransform js/d3)
    :zoom (.-interpolateZoom js/d3)
    (throw (js/Error. "Unknown interpolate type"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Scale

(defn scale [scale-type & {:as opts}]
  (let [ctor (aget js/d3 "scale" (name scale-type))
        obj (ctor)]
    (doseq [[k v] opts
            :let [v (cond
                     (and (= :interpolate k) (keyword? v)) (interpolate v)
                     :else (clj->js v))]]
      ((aget obj (name k)) v))
    obj))
