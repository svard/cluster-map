(ns cluster-map.comm)

(defn request [{:keys [out] :as conn} req]
  (.println out req)
  (.flush out))

(defn read-response [{:keys [in] :as conn}]
  (.readLine in))

(defn close [{:keys [in out] :as conn}]
  (reset! (:open? conn) false)
  (.close in)
  (.close out))

(defn is-open? [{:keys [open?] :as conn}]
  @open?)
