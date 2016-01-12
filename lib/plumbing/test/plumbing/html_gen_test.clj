(ns plumbing.html-gen-test
  (:use plumbing.html-gen clojure.test plumbing.test))


(deftest style-test
  (is (= "k:v;k1:v1;"(style {:k "v" :k1 "v1"}))))

(deftest emit-attribute-test
  (is (= "\"v\"" (emit-attribute "blah" "v")))
  (is (= "style=\"width:500;\"" (emit-attributes {:style {:width "500"}}))))


(deftest render-test
  (is (=
       "<p style=\"color:orange;\">Hey there</p>"
       (render
        [:p {:style {:color "orange"}} "Hey there"])))
  (is (= "<div>My div <p style=\"background:grey;\">Bullshit</p></div>"
         (render [:div "My div" [:p {:style {:background "grey"}} "Bullshit"]])))
  (is (= "<p> <p>"
         (render :p :p))))

(deftest og-test
  (is (= "<meta og:title=\"Title\"></meta> <meta og:type=\"Test\"></meta>"
         (render [[:meta {:og:title "Title"}]
                  [:meta {:og:type "Test"}]]))))

(deftest table-test
  (is-= '[:table
          [[:tr ([:th "foo"] [:th "bar"])]
           [:tr {} ([:td 1] [:td ""])]
           [:tr {} ([:td ""] [:td 2])]]]
        (table [:foo :bar] [{:foo 1} {:bar 2}])))
