(ns cluster-map.core
  (:require [clojure.string :as string])
  (:import [java.net Socket ConnectException]
           [java.io PrintWriter InputStreamReader BufferedReader]))

(defprotocol IConnection
  (is-open? [conn])
  (request [conn request])
  (read-response [conn])
  (close [conn]))

(defrecord Connection [socket in out]
  IConnection
  (is-open? [conn]
    @(:open? (meta conn)))
  (request [_ request]
    (.println out request)
    (.flush out))
  (read-response [_]
    (.readLine in))
  (close [conn]
    (reset! (:open? (meta conn)) false)
    (.close in)
    (.close out)))

(defn- connect-cluster [hosts]
  (let [host (:host (first hosts))
        port (:port (first hosts))]
    (try
      (Socket. host port)
      (catch ConnectException ex
        (if (seq (rest hosts))
          (connect-cluster (rest hosts))
          (throw ex))))))

(defn- parse [resp]
  (case (first resp)
    "OK" (second resp)
    "NOK" nil))

(defn- verify [resp]
  (case (first resp)
    "OK" true
    "NOK" false))

(defn connect [hosts]
  (let [socket (connect-cluster hosts)
        in (BufferedReader. (InputStreamReader. (.getInputStream socket)))
        out (PrintWriter. (.getOutputStream socket))]
    (Connection. socket in out {:open? (atom true)} {})))

(defn create-db [conn db]
  (request conn (str "CREATE " db))
  (-> (read-response conn)
      (string/split #" " 2)
      (verify)))

(defn drop-db [conn db]
  (request conn (str "DROP " db))
  (-> (read-response conn)
      (string/split #" " 2)
      (verify)))

(defn put-val [conn db key value]
  (if (keyword? key)
    (request conn (str "PUT " db " " (name key) " " value))
    (request conn (str "PUT " db " " key " " value)))
  (-> (read-response conn)
      (string/split #" " 2)
      (verify)))

(defn get-val [conn db key]
  (if (keyword? key)
    (request conn (str "GET " db " " (name key)))
    (request conn (str "GET " db " " key)))
  (-> (read-response conn)
      (string/split #" " 2)
      (parse)))

(defn delete-val [conn db key]
  (if (keyword? key)
    (request conn (str "DELETE " db " " (name key)))
    (request conn (str "DELETE " db " " key)))
  (-> (read-response conn)
      (string/split #" " 2)
      (verify)))

(defn make-transient [conn db]
  (reify
    clojure.lang.ILookup
    (valAt [this k]
      (when-let [val (get-val conn db k)]
        val))
    (valAt [this k default]
      (or (.valAt this k) default))
    clojure.lang.ITransientMap
    (assoc [this k v]
      (when (put-val conn db k v)
        this))
    (without [this k]
      (when (delete-val conn db k)
        this))))
