(ns explorer-backend.global
  (:require [clojure.string :as str])
  (:import (java.lang.management ManagementFactory)))

(def config (atom {}))

(def pid
  (-> (ManagementFactory/getRuntimeMXBean)
      (.getName)
      (str/split #"@")
      first))
