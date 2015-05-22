# cluster-map

Clojure bindings for distrib_db.

## Usage

Connect to cluster and create a store

```clojure
(def conn (connect [{:host "192.168.0.1" :port 4040} {:host "192.168.0.2" :port 4040}]))

(create-db conn "store")
```
Create a clustered transient map

```clojure
(def cluster (cluster-map conn "store"))

(assoc! cluster :key1 123 :key2 321)

(get cluster :key2)
;; => "321"
```
## License

Copyright © 2015 Kristofer Svärd

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
