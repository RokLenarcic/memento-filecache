(ns memento.filecache
  "Settings for a minimal Datalevin-backed Memento cache."
  (:require [memento.filecache.cache :as cache]))

(def cache
  "Value for :memento.core/type setting for filecache."
  ::cache)

(def db
  "Datalevin KV connection. It can be a KV object, an IDeref, or a 0-arg fn."
  ::db)

(def dir
  "Datalevin KV directory or URI. When provided, this cache opens the KV lazily."
  ::dir)

(def opts
  "Options passed to datalevin.core/open-kv when dir is used."
  ::opts)

(def dbi
  "Datalevin DBI name used for cached entries. Defaults to \"memento-filecache\"."
  ::dbi)

(defn start-tracking!
  "Start tracking cache keys read by this filecache instance. Returns cache."
  [cache]
  (cache/start-tracking! cache))

(defn sweep-untracked!
  "Delete cache entries not seen since start-tracking!. Returns cache."
  [cache]
  (cache/sweep-untracked! cache))
