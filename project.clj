(defproject btspn.journal "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.taoensso/timbre "4.0.2"]
                 [org.clojure/core.match "0.3.0-alpha4"]]
  :aot :all
  :profiles {:dev {:source-paths ["dev"]}})
