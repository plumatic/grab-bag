(ns tubes.core
  (:refer-clojure :exclude [format])
  (:use-macros
   [plumbing.core :only [for-map]])
  (:require-macros
   [tubes.macros :as tubes-macros])
  (:require
   [goog.string :as gstring]
   [goog.string.format]
   [clojure.string :as str]))

(defn format [s & args]
  (apply gstring/format s args))

(defn truncate [text n ellipsis]
  (if (<= (count text) n)
    text
    (str (subs text 0 (- n (count ellipsis))) ellipsis)))

(defn safe-get [m k]
  (if-let [[_ v] (find m k)]
    v
    (throw (str "Map " m " is missing key " k))))

(defn safe-get-in [m ks]
  (reduce (fn [m k] (safe-get m k)) m ks))

(defn swap-in!
  "Like swap! but returns old-val"
  [a f & args]
  (loop []
    (let [old-val @a]
      (if (compare-and-set! a old-val (apply f old-val args))
        old-val
        (recur)))))

(defn log [& more]
  (js/console.log (clj->js more)))

(defn log-str [& args]
  (js/console.log (apply str args)))

(defn clamp [x lower upper]
  (-> x (min upper) (max lower)))

(def request-animation-frame
  (or js/window.requestAnimationFrame
      js/window.mozRequestAnimationFrame
      js/window.webkitRequestAnimationFrame
      js/window.msRequestAnimationFrame))

;; list of TLDs copied from twitter-text v1.9.3: https://github.com/twitter/twitter-text-js/blob/master/twitter-text.js
;; url-regex takes all tokens that have a valid tld as a substring
(def top-level-domains
  ["academy" "accountants" "active" "actor" "aero" "agency" "airforce" "archi" "army" "arpa" "asia" "associates" "attorney" "audio" "autos" "axa" "bar" "bargains" "bayern" "beer" "berlin" "best" "bid" "bike" "bio" "biz" "black" "blackfriday" "blue" "bmw" "boutique" "brussels" "build" "builders" "buzz" "bzh" "cab" "camera" "camp" "cancerresearch" "capetown" "capital" "cards" "care" "career" "careers" "cash" "cat" "catering" "center" "ceo" "cheap" "christmas" "church" "citic" "claims" "cleaning" "clinic" "clothing" "club" "codes" "coffee" "college" "cologne" "com" "community" "company" "computer" "condos" "construction" "consulting" "contractors" "cooking" "cool" "coop" "country" "credit" "creditcard" "cruises" "cuisinella" "dance" "dating" "degree" "democrat" "dental" "dentist" "desi" "diamonds" "digital" "direct" "directory" "discount" "dnp" "domains" "durban" "edu" "education" "email" "engineer" "engineering" "enterprises" "equipment" "estate" "eus" "events" "exchange" "expert" "exposed" "fail" "farm" "feedback" "finance" "financial" "fish" "fishing" "fitness" "flights" "florist" "foo" "foundation" "frogans" "fund" "furniture" "futbol" "gal" "gallery" "gift" "gives" "glass" "global" "globo" "gmo" "gop" "gov" "graphics" "gratis" "green" "gripe" "guide" "guitars" "guru" "hamburg" "haus" "hiphop" "hiv" "holdings" "holiday" "homes" "horse" "host" "house" "immobilien" "industries" "info" "ink" "institute" "insure" "int" "international" "investments" "jetzt" "jobs" "joburg" "juegos" "kaufen" "kim" "kitchen" "kiwi" "koeln" "kred" "land" "lawyer" "lease" "lgbt" "life" "lighting" "limited" "limo" "link" "loans" "london" "lotto" "luxe" "luxury" "maison" "management" "mango" "market" "marketing" "media" "meet" "menu" "miami" "mil" "mini" "mobi" "moda" "moe" "monash" "mortgage" "moscow" "motorcycles" "museum" "nagoya" "name" "navy" "net" "neustar" "nhk" "ninja" "nyc" "okinawa" "onl" "org" "organic" "ovh" "paris" "partners" "parts" "photo" "photography" "photos" "physio" "pics" "pictures" "pink" "place" "plumbing" "post" "press" "pro" "productions" "properties" "pub" "qpon" "quebec" "recipes" "red" "rehab" "reise" "reisen" "ren" "rentals" "repair" "report" "republican" "rest" "reviews" "rich" "rio" "rocks" "rodeo" "ruhr" "ryukyu" "saarland" "schmidt" "schule" "scot" "services" "sexy" "shiksha" "shoes" "singles" "social" "software" "sohu" "solar" "solutions" "soy" "space" "spiegel" "supplies" "supply" "support" "surf" "surgery" "suzuki" "systems" "tattoo" "tax" "technology" "tel" "tienda" "tips" "tirol" "today" "tokyo" "tools" "town" "toys" "trade" "training" "travel" "university" "uno" "vacations" "vegas" "ventures" "versicherung" "vet" "viajes" "villas" "vision" "vlaanderen" "vodka" "vote" "voting" "voto" "voyage" "wang" "watch" "webcam" "website" "wed" "wien" "wiki" "works" "wtc" "wtf" "xxx" "xyz" "yachts" "yokohama" "zone" "дети" "москва" "онлайн" "орг" "сайт" "بازار" "شبكة" "موقع" "संगठन" "みんな" "世界" "中信" "中文网" "公司" "公益" "商城" "商标" "在线" "我爱你" "政务" "机构" "游戏" "移动" "组织机构" "网址" "网络" "集团" "삼성"    "ac" "ad" "ae" "af" "ag" "ai" "al" "am" "an" "ao" "aq" "ar" "as" "at" "au" "aw" "ax" "az" "ba" "bb" "bd" "be" "bf" "bg" "bh" "bi" "bj" "bl" "bm" "bn" "bo" "bq" "br" "bs" "bt" "bv" "bw" "by" "bz" "ca" "cc" "cd" "cf" "cg" "ch" "ci" "ck" "cl" "cm" "cn" "co" "cr" "cu" "cv" "cw" "cx" "cy" "cz" "de" "dj" "dk" "dm" "do" "dz" "ec" "ee" "eg" "eh" "er" "es" "et" "eu" "fi" "fj" "fk" "fm" "fo" "fr" "ga" "gb" "gd" "ge" "gf" "gg" "gh" "gi" "gl" "gm" "gn" "gp" "gq" "gr" "gs" "gt" "gu" "gw" "gy" "hk" "hm" "hn" "hr" "ht" "hu" "id" "ie" "il" "im" "in" "io" "iq" "ir" "is" "it" "je" "jm" "jo" "jp" "ke" "kg" "kh" "ki" "km" "kn" "kp" "kr" "kw" "ky" "kz" "la" "lb" "lc" "li" "lk" "lr" "ls" "lt" "lu" "lv" "ly" "ma" "mc" "md" "me" "mf" "mg" "mh" "mk" "ml" "mm" "mn" "mo" "mp" "mq" "mr" "ms" "mt" "mu" "mv" "mw" "mx" "my" "mz" "na" "nc" "ne" "nf" "ng" "ni" "nl" "no" "np" "nr" "nu" "nz" "om" "pa" "pe" "pf" "pg" "ph" "pk" "pl" "pm" "pn" "pr" "ps" "pt" "pw" "py" "qa" "re" "ro" "rs" "ru" "rw" "sa" "sb" "sc" "sd" "se" "sg" "sh" "si" "sj" "sk" "sl" "sm" "sn" "so" "sr" "ss" "st" "su" "sv" "sx" "sy" "sz" "tc" "td" "tf" "tg" "th" "tj" "tk" "tl" "tm" "tn" "to" "tp" "tr" "tt" "tv" "tw" "tz" "ua" "ug" "uk" "um" "us" "uy" "uz" "va" "vc" "ve" "vg" "vi" "vn" "vu" "wf" "ws" "ye" "yt" "za" "zm" "zw" "мкд" "мон" "рф" "срб" "укр" "қаз" "الاردن" "الجزائر" "السعودية" "المغرب" "امارات" "ایران" "بھارت" "تونس" "سودان" "سورية" "عمان" "فلسطين" "قطر" "مصر" "مليسيا" "پاکستان" "भारत" "বাংলা" "ভারত" "ਭਾਰਤ" "ભારત" "இந்தியா" "இலங்கை" "சிங்கப்பூர்" "భారత్" "ලංකා" "ไทย" "გე" "中国" "中國" "台湾" "台灣" "新加坡" "香港" "한국"])

(def url-pattern
  (re-pattern (str "(https?:\\/\\/)*[\\w-\\.]+(\\.(" (clojure.string/join "|" top-level-domains) "))+(:\\d{1,5})*(\\/[\\w-?=#%\\.]*)*")))

(defn url-seq [^String t]
  (->>
   (re-seq url-pattern t)
   (keep first)))

(def email-pattern
  #"([a-zA-Z+0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+")

(defn email-seq [^String t]
  (->>
   (re-seq email-pattern t)
   (keep first)))

(defn eggdrop-search
  "Assume that (pred start) is true, and pred monotonically
   changes from true to false (or remains true).  Return the largestl
   idx in [start, end] where (pred x)."
  [pred start end]
  (if (= start end)
    start
    (let [mid (int (+ 1 start (/ (- end start) 2)))]
      (if (pred mid)
        (recur pred mid end)
        (recur pred start (dec mid))))))

(defn all-leave-one-out
  ([xs so-far]
     (when-let [x (first xs)]
       (cons [x (concat so-far (rest xs))]
             (all-leave-one-out
              (rest xs)
              (conj so-far x)))))
  ([xs] (all-leave-one-out xs [])))

(defn permutations [coll]
  (if (= 1 (count coll))
    (list coll)
    (for [[item the-rest] (all-leave-one-out coll)
          rest-perm (permutations the-rest)]
      (cons item rest-perm))))

(defn subsequences [coll n]
  (assert (>= (count coll) n))
  (if (zero? n)
    (list nil)
    (let [[first-item & the-rest] coll]
      (concat
       (map (partial cons first-item) (subsequences the-rest (dec n)))
       (when (>= (count the-rest) n)
         (subsequences the-rest n))))))

(defn partitions
  "Returns all possible partitions of choosing n balls from
   (count bin-sizes) bins. Each partition is a sequence of numbers
   of length (count bin-sizes) and each entry is the number taken
   from the corresponding bin.
   Each partition satisfies:
    (= (sum partition) n)
    (every? (map <= partition bin-sizes))

   Returns the empty sequence if no partitions exist."
  [bin-sizes n]
  (if (= 1 (count bin-sizes))
    (when (<= n (first bin-sizes))
      [(list n)])
    (let [[current-bin-size & remaining-bin-sizes] bin-sizes]
      (for [current-take (range (inc (min n current-bin-size)))
            rest-partition (partitions remaining-bin-sizes (- n current-take))]
        (cons current-take rest-partition)))))

(defn cross-product
  "(cross-product [[:a :b] [1 2]]) => ((:a 1) (:a 2) (:b 1) (:b 2))"
  [colls]
  (if (<= (count colls) 1)
    (map list (first colls))
    (for [current-first (first colls)
          possible-tail (cross-product (rest colls))]
      (cons current-first possible-tail))))

(defn millis []
  (js* "+(new Date)"))

(defn distinct-by [f xs]
  (let [s (atom #{})]
    (for [x xs
          :let [id (f x)]
          :when (not (@s id))]
      (do (swap! s conj id)
          x))))

(defn histogram
  [xs]
  (let [h (js-obj)]
    (doseq [x xs] (aset h x (+ (or (aget h x) 0) 1)))
    (js->clj h :keywordize-keys true)))

(defn conj-when
  "Like conj but ignores non-truthy values"
  ([coll x] (if x (conj coll x) coll))
  ([coll x & xs]
     (if xs
       (recur (conj-when coll x)
              (first xs)
              (next xs))
       (conj-when coll x))))

(defn spy [x & [context]]
  (js/console.log
   (str "SPY: " context "\n") (clj->js x))
  x)

(defn spy-deref [x & [context]]
  (js/console.log
   (str "SPY: " context "\n") (clj->js @x))
  x)

(defn abs [x]
  (if (neg? x)
    (- x)
    x))

(defn indexed-keep-first
  "Binary search for first non-nil result of (f item) in v.
   Return [index (f item)].
   Precondition: (f item) must be monotonic."
  [f v]
  (when (seq v)
    (loop [left 0
           right (dec (count v))]
      (if (> left right)
        (when-let [fv (and (< left (count v)) (f (v left)))]
          [left fv])

        (let [mid (quot (+ left right) 2)]
          (if (f (v mid))
            (recur left (dec mid))
            (recur (inc mid) right)))))))

(tubes-macros/import-vars
 plumbing.core
 aconcat
 map-vals
 map-keys
 map-from-keys
 map-from-vals
 assoc-when
 dissoc-in
 interleave-all
 sum)
