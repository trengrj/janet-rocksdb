(import /build/rocksdb :as rocksdb)
(import spork/test :as test)

(test/start-suite "rocksdb basic operations")

(test/assert-docs "rocksdb")

(def db (rocksdb/open "test.db"))

(rocksdb/put db "key" "rocksdb!")

(test/assert (= "rocksdb!" (rocksdb/get db "key")))

(rocksdb/delete db "key")

(test/assert (= "" (rocksdb/get db "key")))

(:put db "foo" "bar")
(:put db "goo" "car")

(test/assert (= "bar" (:get db "foo")))

(with [iter (rocksdb/iter-create db)]
      (do
        (rocksdb/iter-seek-first iter)
        (test/assert (= "foo" (rocksdb/iter-key iter)))
        (rocksdb/iter-next iter)
        (test/assert (= "goo" (rocksdb/iter-key iter)))
        (rocksdb/iter-seek-first iter)
        (test/assert (= "foo" (rocksdb/iter-key iter)))
        ))

(rocksdb/close db)

(rocksdb/destroy "test.db")

(test/end-suite)
