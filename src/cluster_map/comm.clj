(ns cluster-map.comm)

(defn request [conn req]
  (.println (:out conn) req)
  (.flush (:out conn)))

(defn read-response [conn]
  (.readLine (:in conn)))

(defn close [conn]
  (reset! (:open? conn) false)
  (.close (:in conn))
  (.close (:out conn)))

(defn is-open? [conn]
  @(:open? conn))
