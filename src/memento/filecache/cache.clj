(ns memento.filecache.cache
  "Implementation for a minimal Datalevin-backed Memento cache."
  (:require [datalevin.core :as d]
            [memento.base :as b]
            [memento.config :as mc]
            [memento.filecache.bloom :as bloom]
            [memento.filecache :as-alias mf]
            [taoensso.nippy :as nippy])
  (:import (clojure.lang AFn IDeref IFn)
            (java.security MessageDigest)
            (java.util Base64 Base64$Encoder)
            (java.util.concurrent ConcurrentHashMap CountDownLatch)
            (java.util.function Supplier)
            (memento.base EntryMeta ICache Segment)))

(def ^:private sha-256
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (MessageDigest/getInstance "SHA-256")))))

(def ^:private b64url-encoder
  (.withoutPadding (Base64/getUrlEncoder)))

(defn- digest
  ^bytes [v]
  (.digest ^MessageDigest (.get ^ThreadLocal sha-256) (nippy/fast-freeze v)))

(defn- b64url
  [^bytes bytes]
  (.encodeToString ^Base64$Encoder b64url-encoder bytes))

(defn- entry-key
  ^bytes [^Segment segment args cache-key-fn]
  (let [args-key (.invoke (.getKeyFn segment) args)
        args-key (if cache-key-fn (.invoke ^IFn cache-key-fn args-key) args-key)
        segment-id (.getId segment)
        segment-id (if (ifn? segment-id) (str segment-id) segment-id)]
    (digest [segment-id args-key])))

(defn- db-source
  [{::mf/keys [db dir]}]
  (cond
    (and db dir) (throw (ex-info "Specify only one of filecache db or dir" {:keys [::mf/db ::mf/dir]}))
    db :db
    dir :dir
    :else (throw (ex-info "Missing configuration key: filecache db or dir" {:keys [::mf/db ::mf/dir]}))))

(defn- conf-db
  [{::mf/keys [db dir opts] :as conf}]
  (case (db-source conf)
    :db (cond
          (instance? IDeref db) #(deref db)
          (ifn? db) db
          :else (constantly db))
    :dir (let [opened (delay (d/open-kv dir opts))]
           #(deref opened))))

(defn- assert-supported-conf
  [conf]
  (doseq [[k msg] [[mc/ttl "TTL is not supported by memento-filecache"]
                   [mc/fade "Fade is not supported by memento-filecache"]
                   [mc/size< "Size based eviction is not supported by memento-filecache"]]
          :when (contains? conf k)]
    (throw (ex-info msg {:key k :val (get conf k)}))))

(defn- no-cache-result?
  [result]
  (and (instance? EntryMeta result) (.isNoCache ^EntryMeta result)))

(def ^:private nil-value ::nil-value)
(def ^:private not-delivered (Object.))
(def ^:private tracking-fpp 0.03)
(def ^:private sweep-batch-size 5000)

(defn- encode-value
  [v]
  (nippy/freeze (if (nil? v) nil-value v)))

(defn- decode-value
  [bytes]
  (let [v (nippy/thaw bytes)]
    (when-not (= nil-value v)
      v)))

(defn- track-key!
  [tracking ^bytes digest]
  (when-let [tracker @tracking]
    (bloom/put! tracker digest)))

(defn- put-async!
  ([pending db ops]
   (put-async! pending db ops nil))
  ([pending db ops finally-fn]
   (let [write-future (d/transact-kv-async db ops)
         tracked-future (if finally-fn
                          (future
                            (try
                              @write-future
                              (finally
                                (finally-fn))))
                          write-future)]
     (reset! pending tracked-future)
     tracked-future)))

(defn- remove-load!
  [loads load-key load]
  (.remove ^ConcurrentHashMap loads load-key load))

(deftype AltResult [value])

(defprotocol LoadPromiseOps
  (owner-thread! [p])
  (await-load! [p load-key])
  (value-if-delivered [p])
  (deliver-load! [p v])
  (deliver-exception! [p t])
  (delivered? [p])
  (release-load! [p]))

(deftype LoadPromise [^CountDownLatch latch
                      ^:volatile-mutable result
                      ^:volatile-mutable owner]
  LoadPromiseOps
  (owner-thread! [this]
    (locking this
      (set! owner (Thread/currentThread))))
  (await-load! [_ load-key]
    (when (identical? owner (Thread/currentThread))
      (throw (StackOverflowError. (str "Recursive load on key: " load-key))))
    (.await latch)
    (if (instance? AltResult result)
      (let [value (.-value ^AltResult result)]
        (if (instance? Throwable value)
          (throw value)
          value))
      result))
  (value-if-delivered [_]
    (if (some? result)
      (if (instance? AltResult result)
        (let [value (.-value ^AltResult result)]
          (if (instance? Throwable value)
            not-delivered
            value))
        result)
      not-delivered))
  (deliver-load! [_ v]
    (set! result (if (nil? v) (AltResult. nil) v)))
  (deliver-exception! [this t]
    (set! result (AltResult. t)))
  (delivered? [_]
    (some? result))
  (release-load! [_]
    (set! owner nil)
    (.countDown latch))
  Object
  (toString [_]
    (str "#LoadPromise[" result "]")))

(defn- load-promise
  []
  (LoadPromise. (CountDownLatch. 1) nil nil))

(defrecord FileCache [conf ^IFn db-fn dbi-name ^IFn cache-key-fn ^IFn ret-fn ^IFn ret-ex-fn loads pending tracking]
  IDeref
  (deref [this]
    (some-> @pending deref)
    this)

  ICache
  (conf [_] conf)

  (cached [_ segment args]
    (let [k (entry-key segment args cache-key-fn)
          load-key (b64url k)
          p (load-promise)]
      (if-let [prev (.putIfAbsent ^ConcurrentHashMap loads load-key p)]
        (await-load! prev load-key)
        (try
          (owner-thread! p)
          (track-key! tracking k)
          (if-let [stored (d/get-value (db-fn) dbi-name k :bytes :bytes)]
            (let [ret (decode-value stored)]
              (deliver-load! p ret)
              (remove-load! loads load-key p)
              ret)
            (let [result (try
                           (let [raw (AFn/applyToHelper (.getF ^Segment segment) args)]
                             (if ret-fn (.invoke ret-fn args raw) raw))
                           (catch Throwable t
                             (let [wrapped (if ret-ex-fn (.invoke ret-ex-fn args t) t)]
                               (deliver-exception! p wrapped)
                               (remove-load! loads load-key p)
                               (throw wrapped))))
                  unwrapped (EntryMeta/unwrap result)]
              (deliver-load! p unwrapped)
              (if (no-cache-result? result)
                (remove-load! loads load-key p)
                (put-async! pending
                            (db-fn)
                            [[:put dbi-name k (encode-value unwrapped) :bytes :bytes]]
                             #(remove-load! loads load-key p)))
              unwrapped))
          (catch Throwable t
            (when-not (delivered? p)
              (deliver-exception! p t)
              (remove-load! loads load-key p))
            (throw t))
          (finally
            (release-load! p))))))

  (ifCached [_ segment args]
    (let [k (entry-key segment args cache-key-fn)
          load-key (b64url k)
          loaded (if-let [p (.get ^ConcurrentHashMap loads load-key)]
                   (value-if-delivered p)
                   not-delivered)]
      (if-not (identical? not-delivered loaded)
        (do
          (track-key! tracking k)
          loaded)
        (if-let [stored (d/get-value (db-fn) dbi-name k :bytes :bytes)]
          (do
            (track-key! tracking k)
            (decode-value stored))
          b/absent))))

  (invalidate [this _segment]
    (throw (ex-info "Segment invalidation is not supported by memento-filecache" {})))

  (invalidate [this segment args]
    (let [k (entry-key segment args cache-key-fn)]
      @this
      (d/transact-kv (db-fn) [[:del dbi-name k :bytes]])
      this))

  (invalidateAll [this]
    @this
    (d/clear-dbi (db-fn) dbi-name)
    this)

  (invalidateIds [_ _ids]
    (throw (ex-info "Tag invalidation is not supported by memento-filecache" {})))

  (addEntries [this segment args-to-vals]
    (doseq [[args val] args-to-vals
            :when (not (no-cache-result? val))]
      (let [k (entry-key segment (seq args) cache-key-fn)
            load-key (b64url k)
            unwrapped (EntryMeta/unwrap val)
            p (load-promise)]
        (deliver-load! p unwrapped)
        (release-load! p)
        (.put ^ConcurrentHashMap loads load-key p)
        (put-async! pending
                    (db-fn)
                    [[:put dbi-name k (encode-value unwrapped) :bytes :bytes]]
                    #(remove-load! loads load-key p))))
    this)

  (asMap [this]
    @this
    (into {}
          (map (fn [[k v]] [(b64url k) (decode-value v)]))
          (d/get-range (db-fn) dbi-name [:all] :bytes :bytes false)))

  (asMap [_ _segment]
    (throw (ex-info "Segment as-map is not supported by memento-filecache" {}))))

(defmethod b/new-cache ::mf/cache [conf]
  (assert-supported-conf conf)
  (let [db-fn (conf-db conf)
        db (db-fn)
        dbi-name (::mf/dbi conf "memento-filecache")]
    (d/open-dbi db dbi-name {:key-size 33})
    (->FileCache conf db-fn dbi-name (mc/key-fn conf) (mc/ret-fn conf) (mc/ret-ex-fn conf) (ConcurrentHashMap.) (atom nil) (atom nil))))

(defn- assert-filecache
  [cache]
  (when-not (instance? FileCache cache)
    (throw (ex-info "Expected memento.filecache cache" {:cache cache})))
  cache)

(defn- tracking-expected-entries
  [db dbi-name]
  (max 100000
       (+ 1000 (d/entries db dbi-name))))

(defn start-tracking!
  [cache]
  (let [{:keys [db-fn dbi-name tracking] :as cache} (assert-filecache cache)
        db (db-fn)]
    @cache
    (let [expected (tracking-expected-entries db dbi-name)]
      (reset! tracking (bloom/create expected tracking-fpp)))
    cache))

(defn- delete-batch!
  [db dbi-name batch]
  (when (seq batch)
    (d/transact-kv db (mapv (fn [k] [:del dbi-name k :bytes]) batch))))

(defn- sweep-keys!
  [db dbi-name tracker batch-size]
  (let [batch (volatile! [])]
    (d/visit-key-range db dbi-name
                       (fn [k]
                         (when-not (bloom/might-contain? tracker k)
                           (vswap! batch conj k)
                           (when (<= batch-size (count @batch))
                             (delete-batch! db dbi-name @batch)
                             (vreset! batch []))))
                       [:all]
                       :bytes
                       false)
    (delete-batch! db dbi-name @batch)))

(defn sweep-untracked!
  [cache]
  (let [{:keys [db-fn dbi-name pending tracking] :as cache} (assert-filecache cache)
        tracker @tracking]
    (when-not tracker
      (throw (ex-info "Filecache tracking is not active" {})))
    @cache
    (sweep-keys! (db-fn) dbi-name tracker sweep-batch-size)
    (reset! tracking nil)
    cache))
