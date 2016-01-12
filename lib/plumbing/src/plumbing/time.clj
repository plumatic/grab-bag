(ns plumbing.time
  (:use [plumbing.core :only [unchunk]])
  (:import
   [org.joda.time DateTime DateTimeZone Interval Period]
   [org.joda.time.format DateTimeFormat DateTimeFormatter ISODateTimeFormat]))


(set! *warn-on-reflection* true)

(comment
  "The core namespace for date-time operations in the clj-time library.

   Create a DateTime instance with date-time, specifying the year, month, day,
   hour, minute, second, and millisecond:

     => (date-time 1986 10 14 4 3 27 456)
     #<DateTime 1986-10-14T04:03:27.456Z>

   Less-significant fields can be omitted:

     => (date-time 1986 10 14)
     #<DateTime 1986-10-14T00:00:00.000Z>

   Get the current time with (now) and the start of the Unix epoch with (epoch).

   Once you have a date-time, use accessors like hour and sec to access the
   corresponding fields:

     => (hour (date-time 1986 10 14 22))
     22

   The date-time constructor always returns times in the UTC time zone. If you
   want a time with the specified fields in a different time zone, use
   from-time-zone:

     => (from-time-zone (date-time 1986 10 22) (time-zone-for-offset -2))
     #<DateTime 1986-10-22T00:00:00.000-02:00>

   If on the other hand you want a given absolute instant in time in a
   different time zone, use to-time-zone:

     => (to-time-zone (date-time 1986 10 22) (time-zone-for-offset -2))
     #<DateTime 1986-10-21T22:00:00.000-02:00>

   In addition to time-zone-for-offset, you can use the time-zone-for-id and
   default-time-zone functions and the utc Var to constgruct or get DateTimeZone
   instances.

   The functions after? and before? determine the relative position of two
   DateTime instances:

     => (after? (date-time 1986 10) (date-time 1986 9))
     true

   Often you will want to find a date some amount of time from a given date. For
   example, to find the time 1 month and 3 weeks from a given date-time:

     => (plus (date-time 1986 10 14) (months 1) (weeks 3))
     #<DateTime 1986-12-05T00:00:00.000Z>

   An Interval is used to represent the span of time between two DateTime
   instances. Construct one using interval, then query them using within?,
   overlaps?, and abuts?

     => (within? (interval (date-time 1986) (date-time 1990))
                 (date-time 1987))
     true

   To find the amount of time encompased by an interval, use in-secs and
   in-minutes:

     => (in-minutes (interval (date-time 1986 10 2) (date-time 1986 10 14)))
     17280

   Note that all functions in this namespace work with Joda objects or ints. If
   you need to print or parse date-times, see clj-time.format. If you need to
   ceorce date-times to or from other types, see clj-time.coerce.")

(def utc (DateTimeZone/UTC))

(defn ^{:dynamic true} now []
  "Returns a DateTime for the current instant in the UTC time zone."
  (DateTime. ^DateTimeZone utc))

(defn date-time
  "Constructs and returns a new DateTime in UTC.
   Specify the year, month of year, day of month, hour of day, minute if hour,
   second of minute, and millisecond of second. Note that month and day are
   1-indexed while hour, second, minute, and millis are 0-indexed.
   Any number of least-significant components can be ommited, in which case
   they will default to 1 or 0 as appropriate."
  ([year]
     (date-time year 1 1 0 0 0 0))
  ([year month]
     (date-time year month 1 0 0 0 0))
  ([year month day]
     (date-time year month day 0 0 0 0))
  ([year month day hour]
     (date-time year month day hour 0 0 0))
  ([year month day hour minute]
     (date-time year month day hour minute 0 0))
  ([year month day hour minute second]
     (date-time year month day hour minute second 0))
  ([^Integer year ^Integer month ^Integer day ^Integer hour
    ^Integer minute ^Integer second ^Integer millis]
     (DateTime. year month day hour minute second millis ^DateTimeZone utc)))

(defn hour
  "Return the hour of day component of the given DateTime. A time of 12:01am
   will have an hour component of 0."
  [^DateTime dt]
  (.getHourOfDay dt))

(defn minutes
  "Returns a Period representing the given number of minutes."
  [^Integer n]
  (Period/minutes n))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Formatters

(defn formatter
  "Returns a custom formatter for the given date-time pattern."
  ([^String fmts]
     (formatter fmts utc))
  ([^String fmts ^DateTimeZone dtz]
     (.withZone (DateTimeFormat/forPattern fmts) dtz)))

(def formatters
  (into {} (map
            (fn [[k ^DateTimeFormatter f]] [k (.withZone f ^DateTimeZone utc)])
            {:basic-date (ISODateTimeFormat/basicDate)
             :basic-date-time (ISODateTimeFormat/basicDateTime)
             :basic-date-time-no-ms (ISODateTimeFormat/basicDateTimeNoMillis)
             :basic-ordinal-date (ISODateTimeFormat/basicOrdinalDate)
             :basic-ordinal-date-time (ISODateTimeFormat/basicOrdinalDateTime)
             :basic-ordinal-date-time-no-ms (ISODateTimeFormat/basicOrdinalDateTimeNoMillis)
             :basic-time (ISODateTimeFormat/basicTime)
             :basic-time-no-ms (ISODateTimeFormat/basicTimeNoMillis)
             :basic-t-time (ISODateTimeFormat/basicTTime)
             :basic-t-time-no-ms (ISODateTimeFormat/basicTTimeNoMillis)
             :basic-week-date (ISODateTimeFormat/basicWeekDate)
             :basic-week-date-time (ISODateTimeFormat/basicWeekDateTime)
             :basic-week-date-time-no-ms (ISODateTimeFormat/basicWeekDateTimeNoMillis)
             :date (ISODateTimeFormat/date)
             :date-element-parser (ISODateTimeFormat/dateElementParser)
             :date-hour (ISODateTimeFormat/dateHour)
             :date-hour-minute (ISODateTimeFormat/dateHourMinute)
             :date-hour-minute-second (ISODateTimeFormat/dateHourMinuteSecond)
             :date-hour-minute-second-fraction (ISODateTimeFormat/dateHourMinuteSecondFraction)
             :date-hour-minute-second-ms (ISODateTimeFormat/dateHourMinuteSecondMillis)
             :date-opt-time (ISODateTimeFormat/dateOptionalTimeParser)
             :date-parser (ISODateTimeFormat/dateParser)
             :date-time (ISODateTimeFormat/dateTime)
             :date-time-no-ms (ISODateTimeFormat/dateTimeNoMillis)
             :date-time-parser (ISODateTimeFormat/dateTimeParser)
             :hour (ISODateTimeFormat/hour)
             :hour-minute (ISODateTimeFormat/hourMinute)
             :hour-minute-second (ISODateTimeFormat/hourMinuteSecond)
             :hour-minute-second-fraction (ISODateTimeFormat/hourMinuteSecondFraction)
             :hour-minute-second-ms (ISODateTimeFormat/hourMinuteSecondMillis)
             :local-date-opt-time (ISODateTimeFormat/localDateOptionalTimeParser)
             :local-date (ISODateTimeFormat/localDateParser)
             :local-time (ISODateTimeFormat/localTimeParser)
             :ordinal-date (ISODateTimeFormat/ordinalDate)
             :ordinal-date-time (ISODateTimeFormat/ordinalDateTime)
             :ordinal-date-time-no-ms (ISODateTimeFormat/ordinalDateTimeNoMillis)
             :time (ISODateTimeFormat/time)
             :time-element-parser (ISODateTimeFormat/timeElementParser)
             :time-no-ms (ISODateTimeFormat/timeNoMillis)
             :time-parser (ISODateTimeFormat/timeParser)
             :t-time (ISODateTimeFormat/tTime)
             :t-time-no-ms (ISODateTimeFormat/tTimeNoMillis)
             :week-date (ISODateTimeFormat/weekDate)
             :week-date-time (ISODateTimeFormat/weekDateTime)
             :week-date-time-no-ms (ISODateTimeFormat/weekDateTimeNoMillis)
             :weekyear (ISODateTimeFormat/weekyear)
             :weekyear-week (ISODateTimeFormat/weekyearWeek)
             :weekyear-week-day (ISODateTimeFormat/weekyearWeekDay)
             :year (ISODateTimeFormat/year)
             :year-month (ISODateTimeFormat/yearMonth)
             :year-month-day (ISODateTimeFormat/yearMonthDay)
             :rfc822 (formatter "EEE, dd MMM yyyy HH:mm:ss Z")})))

(defn parse
  "Returns a DateTime instance in the UTC time zone obtained by parsing the
   given string according to the given formatter."
  ([^DateTimeFormatter fmt ^String s]
     (.parseDateTime fmt s))
  ([^String s]
     (first
      (for [f (unchunk (vals formatters))
            :let [d (try (parse f s) (catch Exception _ nil))]
            :when d] d))))

(defn unparse
  "Returns a string representing the given DateTime instance in UTC and in the
  form determined by the given formatter."
  [^DateTimeFormatter fmt ^DateTime dt]
  (.print fmt dt))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Coercions

(defn to-long
  "Returns the number of milliseconds that the given DateTime is after Unix
   epoch."
  [^DateTime dt]
  (.getMillis dt))

(defn from-long
  "Returns a DateTime instance in the UTC time zone corresponding to the given
   number of milliseconds after the Unix epoch."
  [^Long millis]
  (DateTime. millis ^DateTimeZone utc))

(defn to-string
  "Returns a string representation of date in UTC time-zone using
   (ISODateTimeFormat/dateTime) date-time representation. "
  [^DateTime dt]
  (unparse (:date-time formatters) dt))


(set! *warn-on-reflection* false)
