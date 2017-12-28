(defpackage :msgpack-test
  (:use :cl :msgpack :fiveam))

(in-package :msgpack-test)

(def-suite :msgpack)
(in-suite :msgpack)

(defun octets-equal (octets1 octets2)
  (and (= (length octets1) (length octets2))
       (loop :for o1 :across octets1
             :for o2 :across octets2
             :always (= o1 o2))))

(defun to-octets (elements)
  (make-array (length elements)
              :initial-contents elements
              :element-type '(unsigned-byte 8)))

(defun to-hash (&rest plist)
  (let ((hash (make-hash-table :test 'equal)))
    (loop :for (k v) :on plist :by #'cddr
          :do (setf (gethash k hash) v))
    hash))

(defun random-range (min max)
  (+ min (random (- max min))))

(defun random-string (length)
  (let ((string (make-string length)))
    (dotimes (i length)
      (setf (aref string i)
            (code-char (random-range (char-code #\a) (char-code #\z)))))
    string))

(defun plist-to-hash-table (plist)
  (let ((table (make-hash-table :test 'equal)))
    (loop :for (k v) :on plist :by #'cddr
          :do (setf (gethash k table) v))
    table))

(defun hash-equal (hash1 hash2)
  (and (= (hash-table-count hash1) (hash-table-count hash2))
       (progn
         (maphash (lambda (k v)
                    (unless (equalp v (gethash k hash2))
                      (return-from hash-equal nil)))
                  hash1)
         t)))

(test serialize
  (is (octets-equal (serialize nil) (vector #xC0)))
  (is (octets-equal (serialize 'false) (vector #xC2)))
  (is (octets-equal (serialize 'true) (vector #xC3)))
  (is (octets-equal (serialize 0) (vector 0)))
  (is (octets-equal (serialize 1) (vector 1)))
  (is (octets-equal (serialize 2) (vector 2)))
  (is (octets-equal (serialize 127) (vector 127)))
  (is (octets-equal (serialize 128) (vector #xCC 128)))
  (is (octets-equal (serialize -1) (vector #b11111111)))
  (is (octets-equal (serialize -2) (vector #b11111110)))
  (is (octets-equal (serialize -32) (vector #b11100000)))
  (is (octets-equal (serialize 255) (vector #xCC 255)))
  (is (octets-equal (serialize 256) (vector #xCD 1 0)))
  (is (octets-equal (serialize (+ #xFFFF #xAA)) (vector #xCE 0 1 0 169)))
  (is (octets-equal (serialize (+ #xFFFFFFFF #xFEEFBABC02FF))
                    (vector #xCF 0 0 254 240 186 188 2 254)))
  (is (octets-equal (serialize -100) (vector #xD0 (ldb (byte 8 0) -100))))
  (is (octets-equal (serialize -123) (vector 208 133)))
  (is (octets-equal (serialize -250) (vector 209 255 6)))
  (is (octets-equal (serialize -1234) (vector 209 251 46)))
  (is (octets-equal (serialize -84123) (vector 210 255 254 183 101)))
  (let ((base #b10100000))
    (is (octets-equal (serialize "") (vector base)))
    (is (octets-equal (serialize "a") (vector (logior base 1) (char-code #\a))))
    (is (octets-equal (serialize "ab") (vector (logior base 2) (char-code #\a) (char-code #\b)))))
  (is (octets-equal (serialize (make-string 32 :initial-element #\a))
                    (coerce (list* #xD9 32 (loop :repeat 32 :collect (char-code #\a))) 'vector)))
  (is (octets-equal (serialize (make-string (expt 2 8) :initial-element #\a))
                    (coerce (list* #xDA 1 0 (loop :repeat (expt 2 8) :collect (char-code #\a)))
                            'vector)))
  (is (octets-equal (serialize (make-string (expt 2 16) :initial-element #\a))
                    (coerce (list* #xDB 0 1 0 0 (loop :repeat (expt 2 16) :collect (char-code #\a)))
                            'vector)))
  (is (octets-equal (serialize (to-octets '(1 2 3)))
                    (vector #xC4 3 1 2 3)))
  (is (octets-equal (serialize (to-hash "a" 123))
                    (vector (logior (ash 1 7) 1)
                            161 97 123))))

(test deserialize
  (is (eq (deserialize (vector #xC0)) nil))
  (is (eq (deserialize (vector #xC2)) nil))
  (is (eq (deserialize (vector #xC3)) t))
  (is (= (deserialize (vector 106)) 106))
  (loop :for i :from -32 :to -1
        :do (is (equal (deserialize (serialize i)) i)))
  (loop :repeat 10
        :for n := (+ 128 (random 127))
        :do (is (equal (deserialize (serialize n)) n)))
  (loop :repeat 10
        :for n := (+ (ash (1+ (random (1- #xff))) 8) (random #xff))
        :do (is (equal (deserialize (serialize n)) n)))
  (loop :repeat 10
        :for n := (+ (ash (1+ (random (1- #xff))) (* 3 8))
                     (ash (random #xff) (* 2 8))
                     (ash (random #xff) 8)
                     (random #xff))
        :do (is (equal (deserialize (serialize n)) n)))
  (loop :repeat 10
        :for n := (+ (ash (1+ (random (1- #xff))) (* 7 8))
                     (ash (random #xff) (* 6 8))
                     (ash (random #xff) (* 5 8))
                     (ash (random #xff) (* 4 8))
                     (ash (random #xff) (* 3 8))
                     (ash (random #xff) (* 2 8))
                     (ash (random #xff) 8)
                     (random #xff))
        :do (is (equal (deserialize (serialize n)) n)))
  (is (equal -123 (deserialize (serialize -123))))
  (is (equal -1234 (deserialize (serialize -1234))))
  (is (equal -123456 (deserialize (serialize -123456))))
  (is (equal -12345678 (deserialize (serialize -12345678))))
  (is (equal "hello world" (deserialize (serialize "hello world"))))
  (is (equal "abcdefghijklmnopqrstuvwxyz01234"
             (deserialize (serialize "abcdefghijklmnopqrstuvwxyz01234"))))
  (let ((string (make-string 300 :initial-element #\a)))
    (is (equal string (deserialize (serialize string)))))
  (is (octets-equal (vector 32 120 50)
                    (deserialize
                     (serialize (make-array 3
                                            :initial-contents '(32 120 50)
                                            :element-type '(unsigned-byte 8))))))
  (let* ((length (random-range (expt 2 8) (expt 2 16)))
         (bytes (loop :repeat length
                      :collect (random 256)))
         (octets (make-array length
                             :element-type '(unsigned-byte 8)
                             :initial-contents bytes)))
    (is (equalp octets (deserialize (serialize octets)))))
  (let ((v (vector nil t 1 100 -123 2341134 -1234 "abcdefg")))
    (is (equalp v (deserialize (serialize v)))))
  (let ((v (coerce (loop :repeat (random-range (expt 2 8) (expt 2 16))
                         :collect (random-string (random-range 10 20)))
                   'vector)))
    (is (equalp v (deserialize (serialize v)))))
  (let ((hash (plist-to-hash-table '("foo" 123 "bar" "hogehoge" -123 #(1 2 3)))))
    (let ((hash2 (deserialize (serialize hash))))
      (is (hash-equal hash hash2)))))


(defstruct foo x y)

(defmethod serialize-value ((foo foo))
  (make-ext #x40 (list (foo-x foo) (foo-y foo))))

(test ext-serialize
  (is (octets-equal #(214 64 146 100 204 200)
                    (serialize (make-foo :x 100 :y 200))))
  (is (octets-equal #(215 64 0 0 146 0 163 65 66 67)
                    (serialize (make-foo :x 0 :y "ABC")))))
