

(import spork/test :as test)


(if (= :macos (os/which))
  (do
    (test/start-suite "leak detection")
    (def err (file/open "/dev/null" :w))
    (with [out (file/open "results.txt" :w)]
          (os/execute ["leaks" "-atExit" "--" "janet" "test/rocksdb.janet"] :p {:out out :err err}))
    (def buf (file/read (file/open "results.txt" :r) :all))
    (if (= nil (string/find " 0 leaks for 0 total leaked bytes" buf))
      (do
        (print buf)
        (test/assert nil))
      (test/assert true))
    (os/rm "results.txt")
    (test/end-suite)))
