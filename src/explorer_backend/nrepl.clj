(ns explorer-backend.nrepl
  (:require
   [mount.core :as mount]
   [taoensso.timbre :as log]
   [clojure.tools.nrepl.server :as nrepl]
   [explorer-backend.global :as g]))

(mount/defstate nrepl-server
  :start (let [server (nrepl/start-server :port (:nrepl-port @g/config))]
           (log/info (str "nrepl server started on port: " (:nrepl-port @g/config) "."))
           server)
  :stop (do
          (nrepl/stop-server nrepl-server)
          (log/info "nrepl server stopped.")))
