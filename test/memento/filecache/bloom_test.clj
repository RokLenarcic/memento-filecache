(ns memento.filecache.bloom-test
  (:require [clojure.test :refer :all]
            [memento.filecache.bloom :as bloom])
  (:import (clojure.lang ExceptionInfo)
           (java.security MessageDigest)))

(defn digest
  ^bytes [s]
  (.digest (MessageDigest/getInstance "SHA-256") (.getBytes s "UTF-8")))

(deftest long-from-bytes-uses-unsigned-big-endian-bytes
  (let [long-from-bytes #'bloom/long-from-bytes]
    (is (= ["72623859790382856"
            "18374686479671623680"
            "18446744073709551615"]
           (mapv #(Long/toUnsignedString (long-from-bytes % 0))
                 [(byte-array [1 2 3 4 5 6 7 8])
                  (byte-array [(unchecked-byte 0xff) 0 0 0 0 0 0 0])
                  (byte-array (repeat 8 (unchecked-byte 0xff)))])))))

(deftest put-then-might-contain
  (let [tracker (bloom/create 100 0.000001)]
    (bloom/put! tracker (digest "tracked"))
    (is (bloom/might-contain? tracker (digest "tracked")))))

(deftest unrelated-digest-is-definitely-absent-at-small-scale
  (let [tracker (bloom/create 1000 0.000001)]
    (bloom/put! tracker (digest "tracked"))
    (is (false? (bloom/might-contain? tracker (digest "untracked"))))))

(deftest create-validates-settings
  (are [expected fpp]
    (thrown? ExceptionInfo (bloom/create expected fpp))
    0 0.1
    100 0.0
    100 1.0))
