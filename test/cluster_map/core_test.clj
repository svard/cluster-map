(ns cluster-map.core-test
  (:require [clojure.test :refer :all]
            [cluster-map.core :refer [with-connection] :as cm]
            [cluster-map.comm :as comm]
            [cluster-map.spec]))

((resolve 'clojure.spec/instrument-ns) 'cluster-map.core)

(def test-server {:hostname "192.168.0.187" :port 4040})
(def test-db (str "test-db"))

(defn- clean-up [t]
  (t)
  (with-connection [conn (cm/connect test-server)]
    (when (cm/exist? conn test-db)
      (cm/drop-db conn test-db))))

(use-fixtures :each clean-up)

(deftest create-db 
  (with-connection [conn (cm/connect test-server)]
    (is (= true (cm/create-db conn test-db)))))

(deftest drop-db
  (with-connection [conn (cm/connect test-server)]
    (cm/create-db conn test-db)
    (is (= true (cm/drop-db conn test-db)))))

(deftest put-and-get-val
  (testing "keyword key"
    (with-connection [conn (cm/connect test-server)]
      (cm/create-db conn test-db)
      (is (= true (cm/put-val conn test-db :my-key 42)))
      (is (= 42 (cm/get-val conn test-db :my-key)))))

  (testing "string key"
    (with-connection [conn (cm/connect test-server)]
      (cm/create-db conn test-db)
      (is (= true (cm/put-val conn test-db "my-key" 42)))
      (is (= 42 (cm/get-val conn test-db "my-key"))))))

(deftest delete-val
  (testing "keyword key"
    (with-connection [conn (cm/connect test-server)]
      (cm/create-db conn test-db)
      (cm/put-val conn test-db :my-key 42)
      (cm/delete-val conn test-db :my-key)
      (is (= nil (cm/get-val conn test-db :my-key)))))

  (testing "string key"
    (with-connection [conn (cm/connect test-server)]
      (cm/create-db conn test-db)
      (cm/put-val conn test-db "my-key" 42)
      (cm/delete-val conn test-db "my-key")
      (is (= nil (cm/get-val conn test-db "my-key"))))))

(deftest map-datatype
  (testing "assoc! value"
    (with-connection [conn (cm/connect test-server)]
      (let [cluster (cm/cluster-map conn test-db)]
        (cm/create-db conn test-db)
        (assoc! cluster :my-key 42)
        (is (= 42 (get cluster :my-key))))))

  (testing "dissoc! value"
    (with-connection [conn (cm/connect test-server)]
      (let [cluster (cm/cluster-map conn test-db)]
        (cm/create-db conn test-db)
        (assoc! cluster :my-key 42)
        (is (= 42 (get cluster :my-key)))
        (dissoc! cluster :my-key)
        (is (= nil (get cluster :my-key)))))))
