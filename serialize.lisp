(in-package :msgpack)

(defgeneric serialize-value (value))

(defvar *serialize-buffer*)

(defun put-octet (octet)
  (vector-push-extend octet *serialize-buffer*))

(defun put-octets (octets)
  (loop :for octet :across octets :do (put-octet octet)))

(defun put-big-endian (n value)
  (loop :for i :downfrom (1- n) :to 0
        :for pos := (* i 8)
        :collect (put-octet (ldb (byte 8 pos) value))))

(defun put-vector (vector)
  (loop :for value :across vector :do (serialize-aux value)))

(defun put-sequence (sequence)
  (map nil
       (lambda (v) (serialize-aux v))
       sequence))

(defun put-hash-table (hash)
  (maphash (lambda (k v)
             (serialize-aux k)
             (serialize-aux v))
           hash))

(defun serialize-sequence (sequence)
  (let ((len (length sequence)))
    (etypecase len
      ((unsigned-byte 4)
       (put-octet (logior #b10010000 len))
       (put-sequence sequence))
      ((unsigned-byte 16)
       (put-octet #xDC)
       (put-big-endian 2 len)
       (put-sequence sequence))
      ((unsigned-byte 32)
       (put-octet #xDD)
       (put-big-endian 4 len)
       (put-sequence sequence)))))

(defun serialize-aux (value)
  (typecase value
    (null
     (put-octet #xC0))

    ((eql t)
     (put-octet #xC3))

    ((eql false)
     (put-octet #xC2))

    ((eql true)
     (put-octet #xC3))

    ((integer 0 127)
     (put-octet value))

    ((integer -32 -1)
     (put-octet (logior #b11100000 (logand #b11111 value))))

    ((unsigned-byte 8)
     (put-octet #xCC)
     (put-octet value))

    ((unsigned-byte 16)
     (put-octet #xCD)
     (put-big-endian 2 value))

    ((unsigned-byte 32)
     (put-octet #xCE)
     (put-big-endian 4 value))

    ((unsigned-byte 64)
     (put-octet #xCF)
     (put-big-endian 8 value))

    ((signed-byte 8)
     (put-octet #xD0)
     (put-big-endian 1 (ldb (byte 8 0) value)))

    ((signed-byte 16)
     (put-octet #xD1)
     (put-big-endian 2 (ldb (byte 16 0) value)))

    ((signed-byte 32)
     (put-octet #xD2)
     (put-big-endian 4 (ldb (byte 32 0) value)))

    ((signed-byte 64)
     (put-octet #xD3)
     (put-big-endian 8 (ldb (byte 64 0) value)))

    (float
     (put-octet #xCA)
     (let ((bits (ieee-floats:encode-float64 value)))
       (put-big-endian 8 bits)))

    (string
     (let* ((octets (babel:string-to-octets value))
            (len (length octets)))
       (etypecase len
         ((unsigned-byte 5)
          (put-octet (logior #b10100000 len))
          (put-octets octets))
         ((unsigned-byte 8)
          (put-octet #xD9)
          (put-octet len)
          (put-octets octets))
         ((unsigned-byte 16)
          (put-octet #xDA)
          (put-big-endian 2 len)
          (put-octets octets))
         ((unsigned-byte 32)
          (put-octet #xDB)
          (put-big-endian 4 len)
          (put-octets octets)))))

    ((simple-array (unsigned-byte 8))
     (let ((len (length value)))
       (etypecase len
         ((unsigned-byte 8)
          (put-octet #xC4)
          (put-octet len)
          (put-octets value))
         ((unsigned-byte 16)
          (put-octet #xC5)
          (put-big-endian 2 len)
          (put-octets value))
         ((unsigned-byte 32)
          (put-octet #xC6)
          (put-big-endian 4 len)
          (put-octets value)))))

    (vector
     (serialize-sequence value))

    (list
     (serialize-sequence value))

    (hash-table
     (let ((len (hash-table-count value)))
       (etypecase len
         ((unsigned-byte 4)
          (put-octet (logior #b10000000 len))
          (put-hash-table value))
         ((unsigned-byte 16)
          (put-octet #xDE)
          (put-big-endian 2 len)
          (put-hash-table value))
         ((unsigned-byte 32)
          (put-octet #xDF)
          (put-big-endian 4 len)
          (put-hash-table value)))))

    (otherwise
     (serialize-aux (serialize-value value)))))

(defun serialize (value)
  (let ((*serialize-buffer*
         (make-array 0
                     :fill-pointer 0
                     :adjustable t
                     :element-type '(unsigned-byte 8))))
    (serialize-aux value)
    *serialize-buffer*))
