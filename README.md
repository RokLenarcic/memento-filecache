# Memento Filecache

Extremely limited Memento cache backed by Datalevin KV.

This implementation is intentionally narrow:

- permanent entries only
- single JVM load coalescing only
- invalidation by exact key only
- optional full cache clear
- no segment invalidation
- no tag invalidation
- no TTL, fade, or size eviction
- no retained original keys, segment IDs, segment indexes, or tag indexes

## Usage

```clojure
(ns example
  (:require [memento.core :as m]
            [memento.config :as mc]
            [memento.filecache :as mf]))

(def cached
  (m/memo expensive-fn
          {mc/type mf/cache
           mf/dir "/var/cache/my-app/memento"}))
```

You can also pass a caller-managed Datalevin KV connection with `mf/db`.

## Tracking And Sweep

`memento-filecache` can track keys read by one cache instance and later delete entries that were not seen during that tracking window.

```clojure
(mf/start-tracking! (m/active-cache cached))

;; Run normal application traffic here. `cached` and `if-cached` hits are tracked.

(mf/sweep-untracked! (m/active-cache cached))
```

Tracking uses an in-memory Bloom filter. False positives are possible, so sweep may keep an unread entry, but it will not delete an entry that was definitely read by this process after `start-tracking!`.

Tracking sizes the Bloom filter from the current cache entry count when `start-tracking!` is called, using `max(100000, current-entry-count + 1000)`.

The Bloom filter uses Guava's default `0.03` false-positive probability. `memo-add!` writes are not tracked as reads. `sweep-untracked!` deletes in fixed internal batches and disables tracking after it completes.
