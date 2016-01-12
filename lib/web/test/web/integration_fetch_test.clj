(ns web.integration-fetch-test
  "This test is meant to be run when we make major changes to our fetching infrastructure."
  (:use clojure.test plumbing.core plumbing.test)
  (:require
   [clojure.string :as str]
   [plumbing.parallel :as parallel]
   [web.client :as client]))

;; These URL's were chosen from the newsfeed index
;; Picked 20 with the highest dwell. ~30 with the most views. ~12 with the most comments. ~12 with the most clicks
;; and about 30 picked by hand randomly from here and there.
(def +urls+ ["http://jalopnik.com/here-s-what-happened-when-i-tried-to-sell-my-ferrari-to-1652384136"
             "http://www.redmondpie.com/seven-new-ios-8-widgets-worth-trying-out-today-on-your-iphone/"
             "http://www.technologyreview.com/view/532156/googles-secretive-deepmind-start-up-unveils-a-neural-turing-machine/"
             "http://www.dailymail.co.uk/tvshowbiz/article-469646/I-felt-raped-Brando.html"
             "http://www.makeuseof.com/tag/how-to-use-evernote-the-missing-manual-full-text/"
             "http://www.nydailynews.com/life-style/incredible-shrinking-sharpton-article-1.1988923"
             "https://firstlook.org/theintercept/2014/10/28/smuggling-snowden-secrets/"
             "http://www.techlicious.com/blog/4-ways-youre-using-your-tech-completely-wrong/"
             "http://www.iphoneincanada.ca/tips-tricks/170/"
             "http://beforeitsnews.com/alternative/2014/10/trips-to-mars-may-be-off-the-sun-has-changed-in-a-way-weve-never-seen-3052178.html"
             "http://www.connectedrogers.ca/how-to/6-cool-things-to-try-with-your-iphone-6/"
             "http://www.fool.com/investing/general/2014/10/29/why-microsofts-surface-pro-3-sales-soared-as-apple.aspx"
             "http://www.mamamia.com.au/parenting/10-year-old-lies-about-age/"
             "http://www.bloomberg.com/news/2014-10-29/while-you-were-getting-worked-up-over-oil-prices-this-just-happened-to-solar.html"
             "http://venturebeat.com/2014/10/28/microsoft-finally-killed-cloud-storage-whats-next/"
             "http://www.p4rgaming.com/twitch-reinvents-itself-as-a-game-streaming-site-after-banning-sexually-suggestive-clothing/"
             "http://nationalreport.net/iphone-6-facing-massive-recall/"
             "http://www.cultofmac.com/300916/ipad-mini-3-review/"
             "http://www.businessinsider.com/5-of-the-wealthiest-american-families-who-have-lost-their-fortunes-2014-10"
             "http://theunboundedspirit.com/10-things-you-should-always-remember/"
             "http://www.imore.com/how-free-storage-space-your-iphone-or-ipad-icloud-photo-library"
             "http://www.techsling.com/2014/10/business-on-the-go-7-of-the-best-apps-of-2014/"
             "http://www.wired.com/2014/10/meteor/"
             "http://www.fastcodesign.com/3037674/if-smart-design-cant-make-it-in-silicon-valley-who-can"
             "http://www.businessinsider.my/what-apm-graduates-are-doing-now-2014-10/"
             "http://www.bgr.in/news/oppo-r5-is-the-world-thinnest-smartphone-but-at-what-price/"
             "http://www.cnet.com/au/how-to/how-to-fix-apples-imessage-bug/"
             "http://mashable.com/2014/10/28/elon-musk-orbital-sciences-rocket/"
             "http://www.pcmag.com/article2/0%2c2817%2c2471112%2c00.asp"
             "http://www.businessinsider.com/why-people-are-willing-to-pay-5000-to-go-to-conferences-like-wsjd-live-2014-10"
             "http://www.makeuseof.com/tag/6-superb-reasons-use-linux-programming/"
             "https://www.moovooz.com/top-best-iphone-and-android-apps/"
             "http://aplus.com/a/photographer-takes-photos-of-people-right-after-making-out-with-them"
             "http://www.magicalmaths.org/wow-download-the-ivisualiser-app-and-turn-your-ipad-into-a-visualiser/"
             "http://webdesignledger.com/tips/drafting-tips-for-wireframe-sketches"
             "http://www.redmondpie.com/apple-ios-8.1-vs-google-android-5.0-lollipop-visual-comparison-screenshots/"
             "http://www.typewolf.com/blog/will-google-fonts-ever-be-shut-down"
             "http://gizmodo.com/the-way-we-deal-with-photos-is-broken-this-company-wan-1651609426"
             "http://java.dzone.com/articles/career-planning-where-do-old"
             "http://www.redmondpie.com/forcereach-lets-you-customize-and-extend-capabilities-of-iphone-6-6-plus-reachability-feature/"
             "http://www.redmondpie.com/this-could-be-the-reason-why-apple-didnt-name-its-smartwatch-the-iwatch/"
             "http://www.iphonehacks.com/2014/10/learn-build-iphone-games-cook-get-tools-mac-devs-entrepreneurs-free-deals-hub.html"
             "http://homesthetics.net/terrify-guests-ghoulish-great-diy-head-jar-halloween-project/"
             "http://9to5mac.com/2014/10/29/apple-making-ios-8-notification-center-a-bit-less-useful-by-banning-calculator-widgets/"
             "http://blog.pickcrew.com/live-like-youre-dying/"
             "http://gizmodo.com/darpas-chipset-runs-an-astonishing-1-trillion-cycles-pe-1651894585"
             "http://pando.com/2014/10/28/wealthfront-ceo-adam-nash-explains-his-plans-for-that-100-million-pile-of-cash/"
             "http://totallyfunnypixs.com/52561"
             "http://ca.autoblog.com/2014/10/21/top-10-best-apps-most-useful-auto-car-related-nav-maps-repair-fuel-mileage/"
             "http://www.networkedblogs.com/p/10YeFi"
             "http://grahamcluley.com/2014/10/verizon-gives-120-million-customers-cookie-cant-delete/"
             "http://www.stuff.co.za/samsung-takes-a-shot-at-apple-using-steve-jobs-words-to-do-it/"
             "http://www.lifewithdogs.tv/2014/10/loyal-police-dog-being-treated-inhumanely-on-chiefs-orders/"
             "http://www.thekitchn.com/how-to-make-homemade-candy-corn-cooking-lessons-from-the-kitchn-99717"
             "http://www.dailymail.co.uk/news/article-2812587/EXCLUSIVE-watched-horror-vile-dog-meat-trader-butchered-two-family-pets-feet-boiled-alive-Horrifying-defiance-gangsters-murder-man-s-best-friend.html"
             "http://fourhourworkweek.com/2014/10/29/the-books-that-shaped-billionaires-mega-bestselling-authors-and-other-prodigies/"
             "http://www.dailymail.co.uk/news/article-2008119/Crew-flies-Boeings-new-747-Seattle-Pittsburgh-sandwiches.html"
             "http://aquent.com.au/blog/user-experience-roi"
             "http://www.thedailyskid.com/?p=1917"
             "https://iso.500px.com/time-stack-photo-tutorial/"
             "http://appleinsider.com/articles/14/10/29/samsung-suffered-a-739-drop-in-q3-mobile-profits-while-apples-rose-113-percent"
             "http://events.nationalgeographic.com/films/2014/11/07/night-telluride-1/"
             "http://www.cultofmac.com/301422/fcc-rule-change-deliver-apple-tv-youve-dreaming/"
             "http://yournation.org/video-the-shocking-reason-this-man-is-pouring-coke-in-his-car-will-have-you-doing-the-same/"
             "http://io9.com/syfy-basically-admits-they-screwed-up-1651974076"
             "http://nymag.com/thecut/2014/10/see-todd-hidos-haunting-hotel-room-nudes.html"
             "http://valleywag.gawker.com/google-employee-thinks-paying-women-to-date-him-is-phil-1651910849"
             "http://www.jamesbangfiles.com/2014/10/leg-of-girl-who-was-bitten-by-snake.html?m=0"
             "http://venturebeat.com/2014/10/28/how-to-get-googles-inbox-without-an-invite/"
             "http://www.thingsyoumissed.co/10-passionate-reasons-3/"
             "http://bartoszmilewski.com/2014/10/28/category-theory-for-programmers-the-preface/"
             "http://www.businessinsider.in/12-Mind-Blowing-Facts-About-Apple-That-Show-Just-How-Massive-The-Company-Really-Is/articleshow/44966556.cms"
             "https://au.lifestyle.yahoo.com/fashion/features/article/-/25356536/that-s-f-ed-up-rumer-willis-furious-over-photoshop-faux-pas/"
             "http://buzzblips.com/ok-these-15-random-facts-sound-crazy-but-are-actually-very-true/"

             ;; some top publishers randomly collected.
             "http://krugman.blogs.nytimes.com/2014/10/25/notes-on-easy-money-and-inequality"
             "http://www.newyorker.com/magazine/2014/11/03/shut-eat"
             "http://www.bbc.com/news/world-asia-india-29502393"
             "http://qz.com/288544/in-the-charmed-life-of-a-kashmir-cop-servants-tie-shoelaces-and-golf-caddies-carry-carbines/"
             "http://www.thebolditalic.com/articles/6185-the-giants-won-and-the-mission-lost"
             "http://www.washingtonpost.com/opinions/katrina-vanden-heuvel-obama-should-offer-clemency-to-edward-snowden/2014/10/27/b233b054-5dfe-11e4-91f7-5d89b5e8c251_story.html"
             "http://www.latimes.com/nation/la-na-eric-frein-trooper-manhunt-20141030-story.html"
             "http://www.businessweek.com/articles/2014-10-30/tim-cook-im-proud-to-be-gay"
             "https://www.youtube.com/watch?v=aKdV5FvXLuI"
             "http://blogs.hbr.org/2014/10/why-your-brain-loves-good-storytelling/"
             "http://www.theonion.com/articles/2yearold-never-thought-he-would-live-to-see-giants,37320/"
             "http://money.cnn.com/2014/10/30/technology/tim-cook-gay/index.html"
             "https://blog.twitter.com/ibm"
             "http://www.npr.org/event/music/359661053/t-pain-tiny-desk-concert"
             "http://www.theguardian.com/politics/2014/oct/29/bbc-refuses-include-green-party-televised-leader-debates-general-election"
             "http://www.huffingtonpost.ca/reva-seth/reva-seth-jian-ghomeshi_b_6077296.html"
             "https://medium.com/@ewindisch/on-the-security-of-containers-2c60ffe25a9e"
             "http://www.wired.com/2014/10/hackers-using-gmail-drafts-update-malware-steal-data/"
             "http://paleofuture.gizmodo.com/happy-45th-birthday-internet-1651891185"
             "http://sethgodin.typepad.com/seths_blog/2014/10/this-is-not-a-promotion.html"
             "http://www.buzzfeed.com/jimdalrympleii/pennsylvania-police-capture-suspected-cop-killer-eric-frein"
             "http://www.salon.com/2014/10/30/meet_the_woman_whos_turning_your_weirdest_fantasies_into_porn/"
             "http://www.slate.com/articles/news_and_politics/politics/2014/10/republicans_are_turning_west_virginia_red_how_the_democrats_lost_control.html"
             "http://techcrunch.com/2014/10/30/lgs-crazy-new-smartphone-screen-has-almost-no-bezel/"
             "http://www.thedailybeast.com/articles/2014/10/30/time-is-running-out-for-obama-on-syria.html"
             "http://bleacherreport.com/articles/2250369-san-francisco-giants-are-on-an-incredible-run-which-has-us-redefining-dynasty"
             "http://recode.net/2014/10/30/twitter-demotes-product-vp-after-only-six-months/"
             "http://www.vox.com/2014/10/30/7135397/vox-sentences-burkina-faso-jerusalem-ebola-kaci-hickox"

             ])

;; a few of these sites may time out and such. We can never get 100% success rate on all
;; docs on all runs. Just plan for a small number of failures.
(def +acceptable-failure-rate+ 0.03)

(defn- fetch [url]
  [url
   (try
     (if (= 200 (:status (client/fetch :get url))) :success :fail)
     (catch Exception e :exception))])

(deftest ^:integration fetch-test
  (let [summary (->> +urls+ (parallel/map-work 8 fetch) (group-by second))]
    (println (format "Success Rate: %d/%d"
                     (count (:success summary)) (count +urls+)))
    (when (seq (:fail summary))
      (println "FAIL:" (str/join (->> summary :fail (map first)))))
    (when (seq (:exception summary))
      (println "EXCEPTION:" (str/join (->> summary :exception (map first)))))
    (is (> +acceptable-failure-rate+
           (/ (+ (count (:exception summary)) (count (:fail summary)))
              (count +urls+))))))
