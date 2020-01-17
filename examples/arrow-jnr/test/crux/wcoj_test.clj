(ns crux.wcoj-test
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [crux.wcoj :as wcoj]))

(t/deftest test-triangle-join-query
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

(t/deftest test-edge-recursion-rules
  (let [edge '[edge(1, 2).
               edge(2, 3).

               path(X, Y) :- edge(X, Y).
               path(X, Z) :- path(X, Y), edge(Y, Z).]
        db (wcoj/execute-datalog edge)
        result (wcoj/query-by-name db 'path)]
    (t/is (= #{[1 2] [2 3] [1 3]} (set result)))))

(t/deftest test-literal-booleans
  (let [t '[true .]
        db (wcoj/execute-datalog t)
        result (wcoj/query-by-name db 'true)]
    (t/is (= #{[]} (set result)))))

(t/deftest test-fib-using-interop
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

;; http://www.dlvsystem.com/html/The_DLV_Tutorial.html

(t/deftest test-negation-as-failure
  (let [naf '[smoker(john).
              smoker(jack).

              jogger(jill).
              jogger(john).

              healthy(X) :- jogger(X), not smoker(X).]
        db (wcoj/execute-datalog naf)
        result (wcoj/query-by-name db 'healthy)]
    (t/is (= '#{[jill]} (set result)))))

(t/deftest test-employees
  (let [emp '[emp("Jones",   30000, 35, "Accounting").
              emp("Miller",  38000, 29, "Marketing").
              emp("Koch",  2000000, 24, "IT").
              emp("Nguyen",  35000, 42, "Marketing").
              emp("Gruber",  32000, 39, "IT").

              dept("IT",         "Atlanta").
              dept("Marketing",  "New York").
              dept("Accounting", "Los Angeles").

              q1(Ename, Esalary, Dlocation) :- emp(Ename, Esalary, _, D), dept(D, Dlocation), Esalary > 31000 .]
        db (wcoj/execute-datalog emp)
        result (wcoj/query-by-name db 'q1)]
    (t/is (= #{["Koch" 2000000 "Atlanta"] ["Miller" 38000 "New York"]
               ["Gruber" 32000 "Atlanta"] ["Nguyen" 35000 "New York"]}
             (set result)))))

;; https://www.swi-prolog.org/pldoc/man?section=tabling-non-termination
(t/deftest test-connection-recursion-rules
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

;; https://www3.cs.stonybrook.edu/~warren/xsbbook/node14.html
(t/deftest test-xsb-tabling
  (let [avoids '[avoids(Source,Target) :- owes(Source,Target).
                 avoids(Source,Target) :-
                 owes(Source,Intermediate),
                 avoids(Intermediate,Target).

                 owes(andy,bill).
                 owes(bill,carl).
                 owes(carl,bill).]
        db (wcoj/execute-datalog avoids)
        result (wcoj/query-by-name db 'avoids '[andy Y])]
    (t/is (= '#{[andy bill]
                [andy carl]} (set result)))))

;; Some stress-tests from OCaml Datalog:
;; https://github.com/c-cube/datalog/tree/master/tests

(t/deftest test-ancestor
  (t/is (= "ancestor(bob, douglas).
ancestor(bob, john).
ancestor(ebbon, douglas).
ancestor(ebbon, bob).
ancestor(ebbon, john).
ancestor(john, douglas).
"
           (with-out-str
             (wcoj/execute-datalog '[ancestor(A, B) :-
                                     parent(A, B).
                                     ancestor(A, B) :-
                                     parent(A, C),
                                     D = C,
                                     ancestor(D, B).
                                     parent(john, douglas).
                                     parent(bob, john).
                                     parent(ebbon, bob).
                                     ancestor(A, B)?])))))

(t/deftest test-bidi-path
  (t/is (= "path(a, a).
path(a, b).
path(a, c).
path(a, d).
path(b, a).
path(b, b).
path(b, c).
path(b, d).
path(c, a).
path(c, b).
path(c, c).
path(c, d).
path(d, a).
path(d, b).
path(d, c).
path(d, d)."
         (->> (with-out-str
                (wcoj/execute-datalog
                   '[edge(a, b). edge(b, c). edge(c, d). edge(d, a).
                     path(X, Y) :- edge(X, Y).
                     path(X, Y) :- edge(X, Z), path(Z, Y).
                     path(X, Y) :- path(X, Z), edge(Z, Y).
                     path(X, Y)?]))
              (str/split-lines)
                (into (sorted-set))
                (str/join "\n")))))

(t/deftest test-laps
  (t/is (= "permit(rams, store, rams_couch).
permit(will, fetch, rams_couch).
"
           (with-out-str
             (wcoj/execute-datalog
              '[contains(ca, store, rams_couch, rams).
                contains(rams, fetch, rams_couch, will).
                contains(ca, fetch, Name, Watcher) :-
                contains(ca, store, Name, Owner),
                contains(Owner, fetch, Name, Watcher).
                trusted(ca).
                permit(User, Priv, Name) :-
                contains(Auth, Priv, Name, User),
                trusted(Auth).
                permit(User, Priv, Name)?])))))

(t/deftest test-path
  (t/is (= "path(a, a).
path(a, b).
path(a, c).
path(a, d).
path(b, a).
path(b, b).
path(b, c).
path(b, d).
path(c, a).
path(c, b).
path(c, c).
path(c, d).
path(d, a).
path(d, b).
path(d, c).
path(d, d)."
           (->> (with-out-str
                  (wcoj/execute-datalog
                   '[edge(a, b). edge(b, c). edge(c, d). edge(d, a).
                     path(X, Y) :- edge(X, Y).
                     path(X, Y) :- edge(X, Z), path(Z, Y).
                     path(X, Y)?]))
                (str/split-lines)
                (into (sorted-set))
                (str/join "\n")))))

(t/deftest test-pq
  (t/is (= "q(a).
"
           (with-out-str
             (wcoj/execute-datalog
              '[q(X) :- p(X).
                q(a).
                p(X) :- q(X).
                q(X)?])))))

(t/deftest test-rev-path
  (t/is (= "path(a, a).
path(a, b).
path(a, c).
path(a, d).
path(b, a).
path(b, b).
path(b, c).
path(b, d).
path(c, a).
path(c, b).
path(c, c).
path(c, d).
path(d, a).
path(d, b).
path(d, c).
path(d, d)."
           (->> (with-out-str
                  (wcoj/execute-datalog
                   '[edge(a, b). edge(b, c). edge(c, d). edge(d, a).
                     path(X, Y) :- edge(X, Y).
                     path(X, Y) :- path(X, Z), edge(Z, Y).
                     path(X, Y)?]))
                (str/split-lines)
                (into (sorted-set))
                (str/join "\n")))))

(t/deftest test-tc
  (t/is (= "r(a, b).
r(a, c).
"
           (with-out-str
             (wcoj/execute-datalog
              '[r(X, Y) :- r(X, Z), r(Z, Y).
                r(X, Y) :- p(X, Y), q(Y).
                p(a, b).  p(b, d).  p(b, c).
                q(b).  q(c).
                r(a, Y)?])))))

(t/deftest test-true
  (t/is (= "true.
"
           (with-out-str
             (wcoj/execute-datalog
              '[true .
                true ?])))))
