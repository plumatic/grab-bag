(ns plumbing.new-time
  (:use plumbing.core)
  (:require
   [schema.core :as s])
  (:import
   [java.text DateFormat SimpleDateFormat]
   [java.util Calendar Date Locale TimeZone]))

(set! *warn-on-reflection* true)

(defn to-millis
  ([unit]
     (case unit
       (:ms :millis) 1
       (:s :secs :seconds :second) 1000
       (:mins :minutes :minute :min) (* 1000 60)
       (:h :hrs :hours :hour) (* 1000 60 60)
       (:d :ds :days :day) (* 1000 60 60 24)
       (:w :wks :weeks :week) (* 1000 60 60 24 7)
       (:ys :year :years) (* 1000 60 60 24 365)))
  ([num unit] (long (* num (to-millis unit)))))

(defn from-millis
  [num unit] (/ (double num) (to-millis unit)))

(defn convert
  "Convert `num` from `from-unit` to `to-unit` using the same unit keywords as other new-time functions."
  [num from-unit to-unit]
  (-> num (to-millis from-unit) (from-millis to-unit)))

(defn time-ago [num unit]
  (- (millis) (long (to-millis num unit))))

(defn time-since
  ([^long t] (time-since t :ms))
  ([^long t unit] (from-millis (- (millis) (double t)) unit)))

(defn ^java.util.Calendar pst-calendar
  ([]
     (pst-calendar (millis)))
  ([^Long ts]
     (doto (java.util.Calendar/getInstance (java.util.TimeZone/getTimeZone "PST"))
       (.setTime (Date. ts)))))

(defn ^java.util.Calendar utc-calendar []
  (java.util.Calendar/getInstance (java.util.TimeZone/getTimeZone "UTC")))

(s/defn floor-week-date :- Date
  "Returns Date for start of week relative to d"
  [d :- Date]
  (.getTime
   (doto (pst-calendar)
     (.setTime d)
     (.set java.util.Calendar/DAY_OF_WEEK 1))))

(defn floor-day
  "Returns timestamp for beginning of day that is less than
   or equal to the given timestamp."
  ^long [^long m]
  (let [d (Date. m)]
    (.getTimeInMillis
     (doto (utc-calendar)
       (.setTime d)
       (.set java.util.Calendar/MILLISECOND 0)
       (.set java.util.Calendar/SECOND 0)
       (.set java.util.Calendar/MINUTE 0)
       (.set java.util.Calendar/HOUR_OF_DAY 0)))))

(let [sub-day-ms (dec (long (convert 1 :day :millis)))]
  (defn ceil-day
    "Returns timestamp for beginning of day that is greater than
     or equal to the given timestamp."
    ^long [^long m]
    (floor-day (+ m sub-day-ms))))

(defn format-date
  ([^String fmt-str ^Date ts]
     (format-date fmt-str ts (pst-calendar)))
  ([^String fmt-str ^Date ts calendar]
     (.format (doto (SimpleDateFormat. fmt-str)
                (.setCalendar calendar))
              ts)))

(defn parse-date
  ([^String fmt-str ^String date]
     (parse-date fmt-str date (pst-calendar)))
  ([^String fmt-str ^String date calendar]
     (->> date
          (.parse (doto (SimpleDateFormat. fmt-str) (.setCalendar calendar)))
          (.getTime))))

(defn hour-of-day [^java.util.Calendar calendar]
  (.get calendar java.util.Calendar/HOUR_OF_DAY))

(defn pst-day [ts] (format-date "yyyyMMdd" ts (pst-calendar)))
(defn pst-week [ts] (format-date "yyyyMMdd" (floor-week-date ts) (pst-calendar)))
(defn pst-month [ts] (str (format-date "yyyyMM" ts (pst-calendar)) "01"))

(defn pst-date [^long ts] (pst-day (Date. ts)))

(defn pst-date-str->long [s]
  (parse-date "yyyyMMdd" s))

(defn pretty-ms [ms & [format-spec]]
  (let [format-spec (or format-spec "%1.0f")
        s (/ ms 1000.0)
        m (/ s 60.0)
        h (/ m 60.0)
        d (/ h 24.0)]
    (cond (< s 60) (str (format format-spec s) " s")
          (< m 60) (str (format format-spec m) " m")
          (< h 24) (str (format format-spec h) " h")
          :else (str (format format-spec d) " d"))))

(defn pretty-ms-ago [ms]
  (str (pretty-ms (- (millis) ms)) " ago"))

(let [sdf (doto (SimpleDateFormat. "yyyy-MM-dd_HH-mm-ss")
            (.setTimeZone (TimeZone/getTimeZone "UTC")))]
  (defn pretty-sortable-date
    "The current time (UTC) as a string yyyy-MM-dd_HH-mm-ss"
    ([] (pretty-sortable-date (millis)))
    ([^long now] (.format sdf (Date. now)))))

(let [date-instance-ref (atom nil)
      half-day (long (to-millis 0.5 :day))]
  (defn parse-us-date-string
    "Parses strings of the form 2/9/1980 (Feb 9, 1980) and returns timestamp of noon UTC on that date."
    [s]
    (let [date-instance (or @date-instance-ref
                            (reset! date-instance-ref
                                    (doto (DateFormat/getDateInstance DateFormat/SHORT Locale/US)
                                      (.setCalendar (Calendar/getInstance (TimeZone/getTimeZone "UTC"))))))]
      (->> s (.parse ^DateFormat date-instance) (.getTime) (+ half-day)))))

(s/defn to-start-of-week :- long
  "Returns timestamp coresponding to start of week (monday morning) in UTC"
  [ts :- long]
  (let [offset (to-millis 4 :d)
        week-millis (to-millis 7 :d)]
    ;; (((ts - offset) / week-millis) * week-millis) + offset
    (-> ts
        (- offset)
        (quot week-millis)
        (* week-millis)
        (+ offset))))

(let [cal (utc-calendar)]
  (defn utc-week [ts] (format-date "yyyyMMdd" (Date. (to-start-of-week ts)) cal)))

(s/defn prepend-pst-date :- String
  [s :- String]
  (str (pst-date (millis)) "-" s))

(defn time-until-hour
  ([calendar until-hour]
     (time-until-hour calendar until-hour :ms))
  ([calendar until-hour unit]
     (convert
      (mod (- until-hour (hour-of-day (pst-calendar))) 24)
      :hours unit)))

(set! *warn-on-reflection* false)
