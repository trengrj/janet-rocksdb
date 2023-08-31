(declare-project :name "rocksdb"
                 :description "Janet bindings for the rocksdb key value store"
                 :author "John Trengrove")


(def cflags
  (case (os/which)
    :macos '["-I/opt/homebrew/include/"]
    '[]
    ))

(def lflags
  (case (os/which)
    :macos '["-L/opt/homebrew/lib/" "-lstdc++" "-lrocksdb" "-lsnappy" "-lbz2" "-llz4" "-lz"]
    '["-lstdc++" "-lrocksdb" "-lsnappy" "-lbz2" "-llz4" "-lz"]
    ))


(declare-native
  :name "rocksdb"
  :cflags [;default-cflags ;cflags]
  :lflags [;default-lflags ;lflags]
  :source ["main.c"])
