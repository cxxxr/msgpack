(defsystem "msgpack"
  :depends-on ("ieee-floats"
               "babel")
  :serial t
  :components ((:file "msgpack"))
  :in-order-to ((test-op (test-op "msgpack-test"))))

(defsystem "msgpack-test"
  :depends-on ("msgpack"
               "fiveam")
  :pathname "test/"
  :components ((:file "msgpack"))
  :perform (test-op (o c) (symbol-call :fiveam '#:run! :msgpack)))
