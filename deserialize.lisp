(in-package :msgpack)

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
