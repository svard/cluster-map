(ns cluster-map.core
  (:require [clojure.string :as string]
            [cluster-map.comm :as comm])
  (:import [java.net Socket ConnectException]
           [java.io PrintWriter InputStreamReader BufferedReader]))

(defrecord Connection [socket in out open?])

(defn- connect-cluster [{:keys [hostname port]} & hosts]  
  (try
    (Socket. hostname port)
    (catch ConnectException ex
      (if (seq hosts)
        (apply connect-cluster hosts)
        (throw ex)))))

(defn- parse [resp]
  (case (first resp)
    "OK" (second resp)
    "NOK" nil))

(defn- verify [resp]
  (case (first resp)
    "OK" true
    "NOK" false))

(defn connect [host & hosts]
  (let [socket (apply connect-cluster host hosts)
        in (BufferedReader. (InputStreamReader. (.getInputStream socket)))
        out (PrintWriter. (.getOutputStream socket))]
    (->Connection socket in out (atom true))))

(defn create-db
  ([conn db]
   (comm/request conn (str "CREATE " db))
   (-> (comm/read-response conn)
       (string/split #" " 2)
       (verify)))
  
  ([conn db expire]
   (comm/request conn (str "CREATE " db " " expire))
   (-> (comm/read-response conn)
       (string/split #" " 2)
       (verify))))

(defn drop-db [conn db]
  (comm/request conn (str "DROP " db))
  (-> (comm/read-response conn)
      (string/split #" " 2)
      (verify)))

(defn put-val [conn db key value]
  (let [serialized (pr-str value)]
    (if (keyword? key)
      (comm/request conn (str "PUT " db " " (name key) " " value))
      (comm/request conn (str "PUT " db " " key " " value)))
    (-> (comm/read-response conn)
        (string/split #" " 2)
        (verify))))

(defn get-val [conn db key]
  (if (keyword? key)
    (comm/request conn (str "GET " db " " (name key)))
    (comm/request conn (str "GET " db " " key)))
  (when-let [response (-> (comm/read-response conn)
                          (string/split #" " 2)
                          (parse))]
    (read-string response)))

(defn delete-val [conn db key]
  (if (keyword? key)
    (comm/request conn (str "DELETE " db " " (name key)))
    (comm/request conn (str "DELETE " db " " key)))
  (-> (comm/read-response conn)
      (string/split #" " 2)
      (verify)))

(defn exist? [conn db]
  (comm/request conn (str "EXIST " db))
  (when-let [response (-> (comm/read-response conn)
                          (string/split #" " 2)
                          (parse))]
    (read-string response)))

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

(defmacro with-connection [binding & body]
  `(let ~binding
     (try
       (do ~@body)
       (finally
         (comm/close ~(binding 0))))))

(comment
  (defmacro defcluster [name [hosts db]]
    `(def ~name (-> (connect ~hosts)
                    (cluster-map ~db)))))
