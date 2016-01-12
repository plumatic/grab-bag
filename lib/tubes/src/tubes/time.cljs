(ns tubes.time)

(def ms-map
  (let [second 1000
        minute (* second 60)
        hour (* minute 60)
        day (* hour 24)
        week (* day 7)
        year (* day 365)
        month (/ year 12)]
    {:second second :sec second :s second
     :minute minute :min minute :m minute
     :hour hour :h hour
     :day day :d day
     :week week :w week
     :year year :y year
     :month month :mo month}))

(defn ms [n u]
  (* n (ms-map u)))

(defn relative-date [date]
  (let [time (if (number? date) date (.getTime date))
        now (.now js/Date)
        dt (- now time)
        formats [["just now" (ms 0.7 :m)]
                 ["m" (ms 60 :m) (ms 1 :m)]
                 ["h" (ms 1 :d) (ms 1 :h)]
                 ["d" (ms 7 :d) (ms 1 :d)]
                 ["w" (ms 1 :mo) (ms 1 :w)]
                 ["mo" (ms 1 :y) (ms 1 :mo)]
                 ["y" (.-MAX_VALUE js/Number) (ms 1 :y)]]
        format (first (drop-while #(> dt (second %)) formats))]
    (str
     (when (> (count format) 2) (.round js/Math (/ dt (nth format 2))))
     (first format))))
