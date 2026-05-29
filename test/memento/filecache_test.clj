(ns memento.filecache-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [datalevin.core :as d]
            [memento.base :as b]
            [memento.config :as mc]
            [memento.core :as m]
            [memento.filecache :as mf]
            [memento.filecache.cache :as filecache-cache])
  (:import (clojure.lang ExceptionInfo)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(def ^:dynamic *db* nil)

(defn fixture-db [f]
  (let [dir (str (Files/createTempDirectory "memento-filecache-test" (make-array FileAttribute 0)))
        db (d/open-kv dir {:max-dbs 8})]
    (binding [*db* db]
      (try
        (f)
        (finally
          (d/close-kv db)
          (doseq [f (reverse (file-seq (io/file dir)))]
            (.delete f)))))))

(use-fixtures :each fixture-db)

(defn delete-dir! [dir]
  (doseq [f (reverse (file-seq (io/file dir)))]
    (.delete f)))

(defn conf [& {:as extra}]
  (merge {mc/type mf/cache
          mf/db *db*}
         extra))

(deftest rejects-unsupported-eviction
  (are [k v]
    (thrown-with-msg? ExceptionInfo #"not supported" (m/create (assoc (conf) k v)))
    mc/ttl [1 :m]
    mc/fade [1 :m]
    mc/size< 100))

(deftest validates-datalevin-source-config
  (is (thrown-with-msg? ExceptionInfo #"Missing configuration key" (m/create {mc/type mf/cache})))
  (is (thrown-with-msg? ExceptionInfo #"Specify only one" (m/create (assoc (conf) mf/dir "unused")))))

(deftest custom-dbi-is-isolated
  (let [f (m/memo inc (conf mf/dbi "f-dbi"))
        g (m/memo inc (conf mf/dbi "g-dbi"))]
    (is (= {:f 2 :g 2}
           {:f (f 1) :g (g 1)}))
    (m/memo-clear-cache! (m/active-cache f))
    (is (= {:f b/absent :g 2}
           {:f (b/if-cached (m/active-cache f) (.segment f) (seq [1]))
            :g (b/if-cached (m/active-cache g) (.segment g) (seq [1]))}))))

(deftest dir-source-opens-lazy-datalevin-db
  (let [dir (str (Files/createTempDirectory "memento-filecache-dir" (make-array FileAttribute 0)))
        cache (m/create {mc/type mf/cache
                         mf/dir dir
                         mf/opts {:max-dbs 8}})
        f (m/bind inc {} cache)]
    (try
      (is (= 2 (f 1)))
      (is (= 2 (b/if-cached cache (.segment f) (seq [1]))))
      (finally
        (d/close-kv ((:db-fn cache)))
        (delete-dir! dir)))))

(deftest caches-values-and-invalidates-by-key
  (let [cnt (atom 0)
        f (m/memo (fn [x] (swap! cnt inc) (inc x)) (conf))]
    (is (= {:first 2
            :cached 2
            :calls 1
            :stored 2}
           {:first (f 1)
            :cached (f 1)
            :calls @cnt
            :stored (b/if-cached (m/active-cache f) (.segment f) (seq [1]))}))
    (m/memo-clear! f 1)
    (is (= b/absent (b/if-cached (m/active-cache f) (.segment f) (seq [1]))))
    (is (= 2 (f 1)))
    (is (= 2 @cnt))))

(deftest if-cached-does-not-flush-pending-writes
  (let [started (promise)
        release (promise)
        f (m/memo inc (conf))]
    (with-redefs [d/transact-kv-async (fn [_db _ops]
                                        (future
                                          (deliver started true)
                                          @release
                                          :transacted))]
      (try
        (is (= 2 (f 1)))
        @started
        (is (= b/absent
               (deref (future (b/if-cached (m/active-cache f) (.segment f) (seq [2])))
                      1000
                      ::timeout)))
        (is (= 2 (b/if-cached (m/active-cache f) (.segment f) (seq [1]))))
        (finally
          (deliver release true))))))

(deftest memo-add-is-flushed-before-read
  (let [f (m/memo inc (conf))]
    (m/memo-add! f {[1] 100})
    (is (= 100 (f 1)))))

(deftest nil-return-is-cached
  (let [cnt (atom 0)
        f (m/memo (fn [] (swap! cnt inc) nil) (conf))]
    (is (= {:first nil
            :cached nil
            :calls 1}
           {:first (f)
            :cached (f)
            :calls @cnt}))))

(deftest do-not-cache-values-are-not-stored
  (let [cnt (atom 0)
        f (m/memo (fn []
                    (m/do-not-cache (swap! cnt inc)))
                  (conf))]
    (is (= {:first 1
            :second 2
            :stored b/absent}
           {:first (f)
            :second (f)
            :stored (b/if-cached (m/active-cache f) (.segment f) nil)}))))

(deftest key-fn-and-cache-key-fn-shape-digest-key
  (let [cnt (atom 0)
        f (m/memo (fn [_] (swap! cnt inc))
                  {mc/key-fn first}
                  (conf mc/key-fn name))]
    (is (= {:first 1
            :second 1
            :calls 1}
           {:first (f :a)
            :second (f :a)
            :calls @cnt}))))

(deftest ret-fn-transforms-stored-values
  (let [cnt (atom 0)
        f (m/memo (fn [x] (swap! cnt inc) x)
                  (conf mc/ret-fn (fn [_args ret] (* 10 ret))))]
    (is (= {:first 20
            :cached 20
            :calls 1}
           {:first (f 2)
            :cached (f 2)
            :calls @cnt}))))

(deftest ret-ex-fn-transforms-exceptions-and-load-can-retry
  (let [cnt (atom 0)
        f (m/memo (fn []
                    (if (= 1 (swap! cnt inc))
                      (throw (ex-info "boom" {}))
                      :ok))
                  (conf mc/ret-ex-fn (fn [_args t] (ex-info "wrapped" {:cause-message (ex-message t)}))))]
    (is (thrown-with-msg? ExceptionInfo #"wrapped" (f)))
    (is (= :ok (f)))
    (is (= :ok (f)))
    (is (= 2 @cnt))))

(deftest local-single-flight
  (let [cnt (atom 0)
        f (m/memo (fn [x]
                    (swap! cnt inc)
                    (Thread/sleep 100)
                    x)
                  (conf))]
    (is (= {:values [1 1]
            :calls 1}
           {:values [(deref (future (f 1))) (deref (future (f 1)))]
            :calls @cnt}))))

(deftest recursive-same-key-load-throws
  (let [f-ref (atom nil)
        f (m/memo (fn [x] (@f-ref x)) (conf))]
    (reset! f-ref f)
    (is (thrown-with-msg? StackOverflowError #"Recursive load" (f 1)))))

(deftest multiple-functions-sharing-cache-keep-separate-entries
  (let [cache (m/create (conf))
        f (m/bind inc {mc/id :f} cache)
        g (m/bind dec {mc/id :g} cache)]
    (is (= {:f 2 :g 0}
           {:f (f 1) :g (g 1)}))
    (m/memo-clear! f 1)
    (is (= {:f b/absent :g 0}
           {:f (b/if-cached cache (.segment f) (seq [1]))
            :g (b/if-cached cache (.segment g) (seq [1]))}))))

(defn wait-until-released [started release?]
  (deliver started true)
  (while (not @release?)
    (Thread/onSpinWait)))

(deftest key-invalidation-during-load-only-deletes-stored-entry
  (let [cnt (atom 0)
        started (promise)
        release? (atom false)
        f (m/memo (fn [_]
                    (wait-until-released started release?)
                    (swap! cnt inc))
                  (conf))
        load (future (f 1))]
    @started
    (m/memo-clear! f 1)
    (reset! release? true)
    (is (= {:load 1
            :stored 1
            :cached 1
            :calls 1}
           {:load @load
            :stored (b/if-cached (m/active-cache f) (.segment f) (seq [1]))
            :cached (f 1)
            :calls @cnt}))))

(deftest invalidate-all-during-load-only-clears-stored-entries
  (let [cnt (atom 0)
        started (promise)
        release? (atom false)
        f (m/memo (fn [_]
                    (wait-until-released started release?)
                    (swap! cnt inc))
                  (conf))
        load (future (f 1))]
    @started
    (m/memo-clear-cache! (m/active-cache f))
    (reset! release? true)
    (is (= {:load 1
            :stored 1
            :cached 1
            :calls 1}
           {:load @load
            :stored (b/if-cached (m/active-cache f) (.segment f) (seq [1]))
            :cached (f 1)
            :calls @cnt}))))

(deftest as-map-returns-digest-keys
  (let [f (m/memo inc (conf))]
    (is (= 2 (f 1)))
    (is (= 1 (count (b/as-map (m/active-cache f)))))
    (is (string? (ffirst (b/as-map (m/active-cache f)))))))

(deftest values-persist-across-cache-instances
  (let [f (m/memo inc (conf))]
    (is (= 2 (f 1)))
    (is (= 2 (b/if-cached (m/active-cache f) (.segment f) (seq [1]))))
    (let [cache2 (m/create (conf))
          f2 (m/bind inc {mc/id (.getId (.segment f))} cache2)]
      (is (= 2 (b/if-cached cache2 (.segment f2) (seq [1])))))))

(deftest tracking-sweep-keeps-cached-reads-and-deletes-unread-entries
  (let [f (m/memo inc (conf))]
    (is (= {:one 2 :two 3 :three 4}
           {:one (f 1) :two (f 2) :three (f 3)}))
    (is (= 3 (count (b/as-map (m/active-cache f)))))
    (mf/start-tracking! (m/active-cache f))
    (is (= 2 (f 1)))
    (is (= 4 (f 3)))
    (mf/sweep-untracked! (m/active-cache f))
    (is (= {:one 2
            :two b/absent
            :three 4}
           {:one (b/if-cached (m/active-cache f) (.segment f) (seq [1]))
             :two (b/if-cached (m/active-cache f) (.segment f) (seq [2]))
             :three (b/if-cached (m/active-cache f) (.segment f) (seq [3]))}))))

(deftest tracking-expected-entries-uses-current-cache-size
  (are [entry-count expected]
    (with-redefs [d/entries (fn [& _] entry-count)]
      (= expected (#'filecache-cache/tracking-expected-entries nil "dbi")))
    0 100000
    99000 100000
    99001 100001))

(deftest tracking-sweep-keeps-new-computed-reads
  (let [f (m/memo inc (conf))]
    (is (= 2 (f 1)))
    (mf/start-tracking! (m/active-cache f))
    (is (= 3 (f 2)))
    (mf/sweep-untracked! (m/active-cache f))
    (is (= {:one b/absent
            :two 3}
           {:one (b/if-cached (m/active-cache f) (.segment f) (seq [1]))
            :two (b/if-cached (m/active-cache f) (.segment f) (seq [2]))}))))

(deftest tracking-if-cached-hit-keeps-entry-but-miss-does-not-track
  (let [f (m/memo inc (conf))]
    (is (= 2 (f 1)))
    (mf/start-tracking! (m/active-cache f))
    (is (= 2 (b/if-cached (m/active-cache f) (.segment f) (seq [1]))))
    (is (= b/absent (b/if-cached (m/active-cache f) (.segment f) (seq [2]))))
    (m/memo-add! f {[2] 20})
    (mf/sweep-untracked! (m/active-cache f))
    (is (= {:one 2
            :two b/absent}
           {:one (b/if-cached (m/active-cache f) (.segment f) (seq [1]))
            :two (b/if-cached (m/active-cache f) (.segment f) (seq [2]))}))))

(deftest tracking-start-resets-previous-tracked-keys
  (let [f (m/memo inc (conf))]
    (is (= {:one 2 :two 3}
           {:one (f 1) :two (f 2)}))
    (mf/start-tracking! (m/active-cache f))
    (is (= 2 (f 1)))
    (mf/start-tracking! (m/active-cache f))
    (is (= 3 (f 2)))
    (mf/sweep-untracked! (m/active-cache f))
    (is (= {:one b/absent
            :two 3}
           {:one (b/if-cached (m/active-cache f) (.segment f) (seq [1]))
            :two (b/if-cached (m/active-cache f) (.segment f) (seq [2]))}))))

(deftest tracking-empty-sweep-deletes-all-entries-and-disables-tracking
  (let [f (m/memo inc (conf))]
    (is (= {:one 2 :two 3}
           {:one (f 1) :two (f 2)}))
    (mf/start-tracking! (m/active-cache f))
    (mf/sweep-untracked! (m/active-cache f))
    (is (= {:one b/absent
            :two b/absent}
           {:one (b/if-cached (m/active-cache f) (.segment f) (seq [1]))
            :two (b/if-cached (m/active-cache f) (.segment f) (seq [2]))}))
    (is (thrown-with-msg? ExceptionInfo #"not active" (mf/sweep-untracked! (m/active-cache f))))))

(deftest tracking-api-rejects-non-filecache
  (is (thrown-with-msg? ExceptionInfo #"Expected memento.filecache cache" (mf/start-tracking! b/no-cache)))
  (is (thrown-with-msg? ExceptionInfo #"Expected memento.filecache cache" (mf/sweep-untracked! b/no-cache))))

(deftest rejects-segment-and-tag-operations
  (let [f (m/memo inc (conf))]
    (is (= 2 (f 1)))
    (is (thrown-with-msg? ExceptionInfo #"Segment invalidation" (m/memo-clear! f)))
    (is (thrown-with-msg? ExceptionInfo #"Tag invalidation" (b/invalidate-ids (m/active-cache f) [[:tag 1]])))
    (is (thrown-with-msg? ExceptionInfo #"Segment as-map" (m/as-map f)))))

(deftest invalidate-all-is-supported
  (let [f (m/memo inc (conf))]
    (is (= 2 (f 1)))
    (m/memo-clear-cache! (m/active-cache f))
    (is (= b/absent (b/if-cached (m/active-cache f) (.segment f) (seq [1]))))))

(deftest ignores-tag-ids-on-values
  (let [cnt (atom 0)
        f (m/memo (fn [x]
                    (swap! cnt inc)
                    (m/with-tag-id x :tag x))
                  [:tag]
                  (conf))]
    (is (= 1 (f 1)))
    (is (= 1 (f 1)))
    (is (= 1 @cnt))
    (is (thrown-with-msg? ExceptionInfo #"Tag invalidation" (m/memo-clear-tag! :tag 1)))
    (is (= 1 (f 1)))
    (is (= 1 @cnt))))
