(ns memento.filecache.bloom
  "Small concurrent Bloom filter for SHA-256 digest byte arrays."
  (:import (java.util.concurrent.atomic AtomicLongArray)))

(set! *unchecked-math* true)

(def ^:private log-two (Math/log 2.0))
(def ^:private squared-log-two (* log-two log-two))
(def ^:private golden-ratio-hash -7046029254386353131)

(defn- optimal-num-bits
  [expected-entries fpp]
  (long (/ (* (- expected-entries) (Math/log fpp)) squared-log-two)))

(defn- optimal-num-hash-functions
  [fpp]
  (max 1 (long (Math/round (/ (- (Math/log fpp)) log-two)))))

(defn- long-from-bytes
  ^long [^bytes digest ^long offset]
  (let [i (int offset)]
    (bit-or (bit-shift-left (bit-and 0xff (aget digest i)) 56)
            (bit-shift-left (bit-and 0xff (aget digest (unchecked-inc i))) 48)
            (bit-shift-left (bit-and 0xff (aget digest (unchecked-add i 2))) 40)
            (bit-shift-left (bit-and 0xff (aget digest (unchecked-add i 3))) 32)
            (bit-shift-left (bit-and 0xff (aget digest (unchecked-add i 4))) 24)
            (bit-shift-left (bit-and 0xff (aget digest (unchecked-add i 5))) 16)
            (bit-shift-left (bit-and 0xff (aget digest (unchecked-add i 6))) 8)
            (bit-and 0xff (aget digest (unchecked-add i 7))))))

(deftype BloomTracker [^long bit-size ^long hash-count ^AtomicLongArray data]
  Object
  (toString [_]
    (str "#BloomTracker{:bit-size " bit-size ", :hash-count " hash-count "}")))

(defn create
  [expected-entries fpp]
  (when-not (pos? expected-entries)
    (throw (ex-info "tracking expected entries must be positive" {:expected-entries expected-entries})))
  (when-not (and (pos? fpp) (< fpp 1.0))
    (throw (ex-info "tracking false-positive probability must be between 0 and 1" {:fpp fpp})))
  (let [num-bits (optimal-num-bits expected-entries fpp)
        word-count (long (Math/ceil (/ num-bits 64.0)))
        bit-size (unchecked-multiply word-count 64)
        hash-count (optimal-num-hash-functions fpp)]
    (BloomTracker. bit-size hash-count (AtomicLongArray. word-count))))

(defn- index
  ^long [^long bit-size ^long combined]
  (Long/remainderUnsigned combined bit-size))

(defn- set-bit!
  [^AtomicLongArray data ^long bit-index]
  (let [word-index (int (unsigned-bit-shift-right bit-index 6))
        mask (bit-shift-left 1 (int (bit-and bit-index 63)))]
    (loop []
      (let [old (.get data word-index)
            new (bit-or old mask)]
        (when (and (not= old new)
                   (not (.compareAndSet data word-index old new)))
          (recur))))))

(defn- bit-set?
  [^AtomicLongArray data ^long bit-index]
  (let [word-index (int (unsigned-bit-shift-right bit-index 6))
        mask (bit-shift-left 1 (int (bit-and bit-index 63)))]
    (not (zero? (bit-and (.get data word-index) mask)))))

(defn put!
  [^BloomTracker tracker ^bytes digest]
  (let [bit-size (.-bit-size tracker)
        hash-count (.-hash-count tracker)
        data (.-data tracker)
        h1 (long-from-bytes digest 0)
        h2 (let [h (long-from-bytes digest 8)] (long (if (zero? h) golden-ratio-hash h)))]
    (loop [i (long 0)
           combined h1]
      (when (< i hash-count)
        (set-bit! data (index bit-size combined))
        (recur (unchecked-inc i) (+ combined h2))))))

(defn might-contain?
  [^BloomTracker tracker ^bytes digest]
  (let [bit-size (.-bit-size tracker)
        hash-count (.-hash-count tracker)
        data (.-data tracker)
        h1 (long-from-bytes digest 0)
        h2 (let [h (long-from-bytes digest 8)] (long (if (zero? h) golden-ratio-hash h)))]
    (loop [i (long 0)
           combined h1]
      (cond
        (= i hash-count) true
        (bit-set? data (index bit-size combined)) (recur (unchecked-inc i) (+ combined h2))
        :else false))))
