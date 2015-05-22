(ns cluster-map.core
  (:require [clojure.string :as string])
  (:import [java.net Socket ConnectException]
           [java.io PrintWriter InputStreamReader BufferedReader]))

(defprotocol IConnection
  (is-open? [conn])
  (close [conn]))

(defprotocol ICommunicate
  (request [comm request])
  (read-response [comm]))

(defrecord Connection [socket in out open?]
  IConnection
  (is-open? [_]
    @open?)
  (close [_]
    (reset! open? false)
    (.close in)
    (.close out))
  ICommunicate
  (request [_ request]
    (.println out request)
    (.flush out))
  (read-response [_]
    (.readLine in)))

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
    (->Connection socket in out (atom true))))

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
  (let [serialized (pr-str value)]
    (if (keyword? key)
      (request conn (str "PUT " db " " (name key) " " value))
      (request conn (str "PUT " db " " key " " value)))
    (-> (read-response conn)
        (string/split #" " 2)
        (verify))))

(defn get-val [conn db key]
  (if (keyword? key)
    (request conn (str "GET " db " " (name key)))
    (request conn (str "GET " db " " key)))
  (when-let [response (-> (read-response conn)
                          (string/split #" " 2)
                          (parse))]
    (read-string response)))

(defn delete-val [conn db key]
  (if (keyword? key)
    (request conn (str "DELETE " db " " (name key)))
    (request conn (str "DELETE " db " " key)))
  (-> (read-response conn)
      (string/split #" " 2)
      (verify)))

(defn cluster-map [conn db]
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
