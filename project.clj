(defproject cwim "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha12"]
                 [http-kit "2.1.19"]
                 [org.clojure/core.async "0.2.391"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "0.9.0"]]}})
