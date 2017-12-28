(in-package :msgpack)

(defstruct (ext (:constructor %make-ext (type octets)))
  type octets)

(defun make-serialized-ext (type octets)
  (check-type type (signed-byte 8))
  (%make-ext type octets))

(defun make-ext (type value)
  (check-type type (signed-byte 8))
  (let ((octets (serialize value)))
    (assert (< (length octets) #.(expt 2 32)))
    (%make-ext type octets)))
