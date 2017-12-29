(defpackage :msgpack
  (:use :cl)
  (:export
   :make-ext
   :true
   :false
   :serialize-value
   :serialize
   :deserialize
   :define-ext))
(in-package :msgpack)

(defstruct (ext (:constructor %make-ext (type octets)))
  type octets)

(defun make-ext (type value)
  (check-type type (signed-byte 8))
  (let ((octets (serialize value)))
    (assert (< (length octets) #.(expt 2 32)))
    (%make-ext type octets)))


;;; serialize

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

(defun serialize-ext (ext)
  (let* ((type (ext-type ext))
         (octets (ext-octets ext))
         (len (length octets)))
    (flet ((ext-fixed (kind size)
             (put-octet kind)
             (put-octet type)
             (loop :repeat (- size len) :do (put-octet 0))
             (put-octets octets))
           (ext-variadic (kind n)
             (put-octet kind)
             (put-big-endian n len)
             (put-octet type)
             (put-octets octets)))
      (cond ((<= len 1)
             (ext-fixed #xD4 1))
            ((<= len 2)
             (ext-fixed #xD5 2))
            ((<= len 4)
             (ext-fixed #xD6 4))
            ((<= len 8)
             (ext-fixed #xD7 8))
            ((<= len 16)
             (ext-fixed #xD8 16))
            ((< len #.(expt 2 8))
             (ext-variadic #xC7 1))
            ((< len #.(expt 2 16))
             (ext-variadic #xC8 2))
            (t
             (ext-variadic #xC9 4))))))

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

    (ext
     (serialize-ext value))

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


;;; deserialize

(defgeneric deserialize-ext (type octets))

(defstruct reader
  octets
  pos)

(defvar *reader*)

(defun peek-octet ()
  (aref (reader-octets *reader*)
        (reader-pos *reader*)))

(defun skip-octets (n)
  (incf (reader-pos *reader*) n))

(defun read-octet ()
  (prog1 (peek-octet)
    (skip-octets 1)))

(defun read-octets (len)
  (prog1 (make-array len
                     :element-type '(unsigned-byte 8)
                     :displaced-to (reader-octets *reader*)
                     :displaced-index-offset (reader-pos *reader*))
    (skip-octets len)))

(defun decode-big-endian (n)
  (loop :with bits := 0
        :for i :downfrom (1- n) :to 0
        :do (setf (ldb (byte 8 (* 8 i)) bits)
                  (read-octet))
        :finally (return bits)))

(defun decode-unsigned-integer (n)
  (decode-big-endian n))

(defun decode-signed-integer (n)
  (let ((minusp (logbitp 7 (peek-octet)))
        (bits (decode-big-endian n)))
    (if minusp
        (- (1+ (ldb (byte (* n 8) 0) (lognot bits))))
        bits)))

(defun decode-ieee-float (nbits n)
  (let ((bits (decode-big-endian n)))
    (ecase nbits
      (32 (ieee-floats:decode-float32 bits))
      (64 (ieee-floats:decode-float64 bits)))))

(defun decode-utf-8 (length)
  (prog1 (babel:octets-to-string (reader-octets *reader*)
                                 :start (reader-pos *reader*)
                                 :end (+ (reader-pos *reader*) length)
                                 :encoding :utf-8)
    (skip-octets length)))

(defun decode-utf-8* (n)
  (decode-utf-8 (decode-unsigned-integer n)))

(defun decode-byte-array (n)
  (let ((length (decode-unsigned-integer n)))
    (read-octets length)))

(defun decode-generic-array (length)
  (let ((array (make-array length)))
    (dotimes (i length)
      (setf (aref array i) (deserialize-1)))
    array))

(defun decode-generic-array* (n)
  (decode-generic-array (decode-unsigned-integer n)))

(defun decode-map (length)
  (let ((map (make-hash-table :test 'equal)))
    (loop :repeat length
          :do (let ((k (deserialize-1))
                    (v (deserialize-1)))
                (setf (gethash k map) v)))
    map))

(defun decode-map* (n)
  (decode-map (decode-unsigned-integer n)))

(defun decode-ext-fixed (n)
  (let ((type (read-octet))
        (octets (read-octets n)))
    (deserialize-ext type octets)))

(defun decode-ext-variadic (n)
  (let* ((len (decode-unsigned-integer n))
         (type (read-octet))
         (octets (read-octets len)))
    (deserialize-ext type octets)))

(defun deserialize-1 ()
  (let ((octet (read-octet)))
    (cond ((= octet #xC0) nil)
          ((= octet #xC2) nil)
          ((= octet #xC3) t)
          ((not (logbitp 7 octet))
           (ldb (byte 7 0) octet))
          ((= #b111 (ldb (byte 3 5) octet))
           (- (1+ (logand #b11111 (lognot octet)))))
          ((= octet #xCC) (decode-unsigned-integer 1))
          ((= octet #xCD) (decode-unsigned-integer 2))
          ((= octet #xCE) (decode-unsigned-integer 4))
          ((= octet #xCF) (decode-unsigned-integer 8))
          ((= octet #xD0) (decode-signed-integer 1))
          ((= octet #xD1) (decode-signed-integer 2))
          ((= octet #xD2) (decode-signed-integer 4))
          ((= octet #xD3) (decode-signed-integer 8))
          ((= octet #xCA) (decode-ieee-float 32 4))
          ((= octet #xCB) (decode-ieee-float 64 8))
          ((= #b101 (ldb (byte 3 5) octet))
           (decode-utf-8 (ldb (byte 5 0) octet)))
          ((= octet #xD9) (decode-utf-8* 1))
          ((= octet #xDA) (decode-utf-8* 2))
          ((= octet #xDB) (decode-utf-8* 4))
          ((= octet #xC4) (decode-byte-array 1))
          ((= octet #xC5) (decode-byte-array 2))
          ((= octet #xC6) (decode-byte-array 4))
          ((= #b1001 (ldb (byte 4 4) octet))
           (decode-generic-array (ldb (byte 4 0) octet)))
          ((= octet #xDC) (decode-generic-array* 2))
          ((= octet #xDD) (decode-generic-array* 4))
          ((= #b1000 (ldb (byte 4 4) octet)) (decode-map (ldb (byte 4 0) octet)))
          ((= octet #xDE) (decode-map* 2))
          ((= octet #xDF) (decode-map* 4))
          ((= octet #xD4) (decode-ext-fixed 1))
          ((= octet #xD5) (decode-ext-fixed 2))
          ((= octet #xD6) (decode-ext-fixed 4))
          ((= octet #xD7) (decode-ext-fixed 8))
          ((= octet #xD8) (decode-ext-fixed 16))
          ((= octet #xC7) (decode-ext-variadic 1))
          ((= octet #xC8) (decode-ext-variadic 2))
          ((= octet #xC9) (decode-ext-variadic 4))
          (t (error "unexpected byte: ~B" octet)))))

(defun deserialize (octets)
  (let ((*reader* (make-reader :octets octets :pos 0)))
    (deserialize-1)))


(defmacro define-ext (name (&key type) &body specs)
  (let ((serialize-form (rest (assoc :serialize specs)))
        (deserialize-form (rest (assoc :deserialize specs))))
    (let ((value (gensym "VALUE"))
          (type-var (gensym "TYPE")))
      `(progn
         ,(destructuring-bind ((var) &body body) serialize-form
            `(defmethod serialize-value ((,var ,name))
               (let ((,value (progn ,@body)))
                 (make-ext ,type ,value))))
         ,(destructuring-bind ((octets) &body body) deserialize-form
            `(defmethod deserialize-ext ((,type-var (eql ,type)) ,octets)
               ,@body))))))
