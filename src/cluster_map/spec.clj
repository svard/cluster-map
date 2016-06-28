(ns cluster-map.spec
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [cluster-map.core :refer :all]))

(s/def ::socket #(instance? java.net.Socket %))
(s/def ::in #(instance? java.io.BufferedReader %))
(s/def ::out #(instance? java.io.PrintWriter %))

(s/def ::hostname string?)
(s/def ::port int?)
(s/def ::connection
  (s/keys :req-un [::socket ::in ::out ::open?]))
(s/def ::host
  (s/keys :req-un [::hostname ::port]))

(s/fdef connect
  :args (s/cat :hosts (s/+ ::host))
  :ret ::connection)

(s/fdef create-db
  :args (s/cat :conn ::connection
          :db string?
          :expire (s/? int?))
  :ret boolean?)

(s/fdef drop-db
  :args (s/cat :conn ::connection
          :db string?)
  :ret boolean?)

(s/fdef put-val
  :args (s/cat :conn ::connection
          :db string?
          :key (s/or :i int? :s string? :k keyword?)
          :value (s/or :i int? :s string?))
  :ret boolean?)

(s/fdef get-val
  :args (s/cat :conn ::connection
          :db string?
          :key (s/or :i int? :s string? :k keyword?))
  :ret (s/or :i int? :s string?))

(s/fdef delete-val
  :args (s/cat :conn ::connection
          :db string?
          :key (s/or :i int? :s string? :k keyword?))
  :ret boolean?)

(s/fdef exist?
  :args (s/cat :conn ::connection
          :db string?)
  :ref boolean?)
