(ns crux.wcoj-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.wcoj :as wcoj]))

(t/deftest triangle-join-query
  (let [triangle '[r(1, 3).
                   r(1, 4).
                   r(1, 5).
                   r(3, 5).

                   t(1, 2).
                   t(1, 4).
                   t(1, 5).
                   t(1, 6).
                   t(1, 8).
                   t(1, 9).
                   t(3, 2).

                   s(3, 4).
                   s(3, 5).
                   s(4, 6).
                   s(4, 8).
                   s(4, 9).
                   s(5, 2).

                   q(A, B, C) :- r(A, B), s(B, C), t(A, C).]
        db (wcoj/execute-datalog triangle)
        result (wcoj/query-by-name db 'q)]
    (t/is (= #{[1 3 4] [1 3 5] [1 4 6] [1 4 8] [1 4 9] [1 5 2] [3 5 2]}
             (set result)))))

(t/deftest edge-recursion-rules
  (let [edge '[edge(1, 2).
               edge(2, 3).

               path(X, Y) :- edge(X, Y).
               path(X, Z) :- path(X, Y), edge(Y, Z).]
        db (wcoj/execute-datalog edge)
        result (wcoj/query-by-name db 'path)]
    (t/is (= #{[1 2] [2 3] [1 3]} (set result)))))

(t/deftest fib-using-interop
  (let [fib '[fib(0, 0).
              fib(1, 1).

              fib(N, F) :-
                N != 0,
                N != 1,
                N1 :- -(N, 1),
                N2 :- -(N, 2),
                fib(N1, F1),
                fib(N2, F2),
                F :- +(F1 F2).]
        db (wcoj/execute-datalog fib)
        result (wcoj/query-datalog db '[fib(30, F)?])]
    (t/is (= #{[30 832040]} (set result)))))

;; https://www.swi-prolog.org/pldoc/man?section=tabling-non-termination
(t/deftest connection-recursion-rules
  (let [connection '[connection(X, Y) :- connection(X, Z), connection(Z, Y).
                     connection(X, Y) :- connection(Y, X).

                     connection("Amsterdam", "Schiphol").
                     connection("Amsterdam", "Haarlem").
                     connection("Schiphol", "Leiden").
                     connection("Haarlem", "Leiden").]
        db (wcoj/execute-datalog connection)
        result (wcoj/query-by-name db 'connection '["Amsterdam" X])]
    (t/is (= #{["Amsterdam" "Haarlem"]
               ["Amsterdam" "Schiphol"]
               ["Amsterdam" "Amsterdam"]
               ["Amsterdam" "Leiden"]} (set result)))))
