(ns explorer-backend.core
  (:require
    [mount.core :as mount]
    [taoensso.timbre :as log]
    [taoensso.timbre.appenders.3rd-party.rotor :as rotor]
    [explorer-backend.global :as g]
    [explorer-backend.nrepl :refer [nrepl-server]]
    [explorer-backend.web :refer [http-server]]
    [explorer-backend.db :refer [datasource]])
  (:gen-class))

(defn- setup-log []
  (log/merge-config!
    {:level     (if (= (:env @g/config) :prd) :info :debug)
     :appenders {:rotor (rotor/rotor-appender
                          {:path     "backend.log"
                           :max-size 1024000})}})
  (log/handle-uncaught-jvm-exceptions!))

(defn load-config []
  (swap! g/config merge (load-file "conf/config.clj")))

(defn -main []
  (load-config)
  (setup-log)
  (mount/start #'nrepl-server)
  (mount/start #'http-server)
  (mount/start #'datasource)
  (log/info "server started! Running on PID: " g/pid "."))
