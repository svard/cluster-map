(ns cluster-map.core
  (:require [clojure.string :as string]
            [cluster-map.comm :as comm]
            [schema.core :as s])
  (:import [java.net Socket ConnectException]
           [java.io PrintWriter InputStreamReader BufferedReader]))

(defrecord Connection [socket in out open?])

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

(s/defn connect :- Connection
  [hosts :- [{:host s/Str :port s/Int}]]
  (let [socket (connect-cluster hosts)
        in (BufferedReader. (InputStreamReader. (.getInputStream socket)))
        out (PrintWriter. (.getOutputStream socket))]
    (->Connection socket in out (atom true))))

(s/defn create-db :- s/Bool
  ([conn :- Connection db :- s/Str]
   (comm/request conn (str "CREATE " db))
   (-> (comm/read-response conn)
       (string/split #" " 2)
       (verify)))
  
  ([conn :- Connection db :- s/Str expire :- s/Int]
   (comm/request conn (str "CREATE " db " " expire))
   (-> (comm/read-response conn)
       (string/split #" " 2)
       (verify))))

(s/defn drop-db :- s/Bool
  [conn :- Connection db :- s/Str]
  (comm/request conn (str "DROP " db))
  (-> (comm/read-response conn)
      (string/split #" " 2)
      (verify)))

(s/defn put-val :- s/Bool
  [conn :- Connection db :- s/Str key :- s/Any value :- s/Any]
  (let [serialized (pr-str value)]
    (if (keyword? key)
      (comm/request conn (str "PUT " db " " (name key) " " value))
      (comm/request conn (str "PUT " db " " key " " value)))
    (-> (comm/read-response conn)
        (string/split #" " 2)
        (verify))))

(s/defn get-val :- s/Any
  [conn :- Connection db :- s/Str key :- s/Any]
  (if (keyword? key)
    (comm/request conn (str "GET " db " " (name key)))
    (comm/request conn (str "GET " db " " key)))
  (when-let [response (-> (comm/read-response conn)
                          (string/split #" " 2)
                          (parse))]
    (read-string response)))

(s/defn delete-val :- s/Bool
  [conn :- Connection db :- s/Str key :- s/Any]
  (if (keyword? key)
    (request conn (str "DELETE " db " " (name key)))
    (request conn (str "DELETE " db " " key)))
  (-> (read-response conn)
      (string/split #" " 2)
      (verify)))

(s/defn exist? :- s/Bool
  [conn :- Connection db :- s/Str]
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
