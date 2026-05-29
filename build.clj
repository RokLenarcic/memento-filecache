(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
            [org.corfield.build :as bb]))

(def lib 'org.clojars.roklenarcic/memento-filecache)

(defn git-count-revs []
  (try
    (or (b/git-count-revs nil) 0)
    (catch Exception _
      0)))

(def version (format "0.1.%s" (git-count-revs)))

(defn add-defaults [opts]
  (assoc opts :lib lib :version version))

(defn test "Run the tests." [opts]
  (bb/run-tests opts))

(defn ci "Run the CI pipeline of tests and build the JAR." [opts]
  (-> opts
      add-defaults
      test
      bb/clean
      bb/jar))

(defn install "Install the JAR locally." [opts]
  (-> opts
      add-defaults
      bb/install))

(defn deploy "Deploy the JAR to Clojars." [opts]
  (-> opts
      add-defaults
      (assoc :sign-releases? true)
      bb/deploy))
