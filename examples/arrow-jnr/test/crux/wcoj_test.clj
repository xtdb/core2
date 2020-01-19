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
    (t/is (= '#{[jill]} (set result))))

  (let [naf '[canfly(X) :- bird(X), not abnormal(X).
              abnormal(X) :-  wounded(X).
              bird(john).
              bird(mary).
              wounded(john).]
        db (wcoj/execute-datalog naf)
        result (wcoj/query-by-name db 'canfly)]
    (t/is (= '#{[mary]} (set result))))

  (let [naf '[p .

              r :- p, q .
              s :- p, not q .]
        db (wcoj/execute-datalog naf)]
    (t/is (= '#{[]} (set (wcoj/query-by-name db 's))))
    (t/is (= '#{[]} (set (wcoj/query-by-name db 'p))))
    (t/is (= '#{} (set (wcoj/query-by-name db 'r))))))

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

(t/deftest test-disjunction
  (let [disjunction '[left_arm_broken :- not right_arm_broken .
                      right_arm_broken :- not left_arm_broken .
                      can_write :- left_arm_broken .
                      be_angry :- can_write .]
        db (wcoj/execute-datalog disjunction)]

    (let [db (wcoj/execute-datalog db '[left_arm_broken .])]
      (doseq [q '[left_arm_broken can_write be_angry]]
        (t/is (= '#{[]} (set (wcoj/query-by-name db q)))))
      (t/is (= '#{} (set (wcoj/query-by-name db 'right_arm_broken)))))

    (let [db (wcoj/execute-datalog db '[right_arm_broken .])]
      (doseq [q '[left_arm_broken can_write be_angry]]
        (t/is (= '#{} (set (wcoj/query-by-name db q)))))
      (t/is (= '#{[]} (set (wcoj/query-by-name db 'right_arm_broken)))))))

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
ancestor(ebbon, douglas).
ancestor(ebbon, john).
ancestor(bob, john).
ancestor(ebbon, bob).
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

(t/deftest test-sym
  (t/is (= "perm(a, b).
perm(a, c).
perm(b, a).
perm(b, c).
perm(c, a).
perm(c, b)."
           (->> (with-out-str
                  (wcoj/execute-datalog
                   '[sym(a).
                     sym(b).
                     sym(c).
                     perm(X,Y) :- sym(X), sym(Y), X != Y .

                     perm(X,Y)?]))
                (str/split-lines)
                (into (sorted-set))
                (str/join "\n")))))

(t/deftest test-true
  (t/is (= "true.
"
           (with-out-str
             (wcoj/execute-datalog
              '[true .
                true ?])))))

;; https://github.com/fogfish/datalog/blob/master/test/datalog_SUITE.erl

(t/deftest test-erlang-datalog
  (t/testing "single horn 1"
    (t/is (= #{[1 2] [2 3] [3 4] [4 5]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 4] [4 5]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "single horn 2"
    (t/is (= #{[1 3] [2 4] [3 5]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Z), p(Z, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 4] [4 5]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "single horn 3"
    (t/is (= #{[1 4] [2 5]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Z), p(Z, F), p(F, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 4] [4 5]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "horn 2"
    (t/is (= #{[1 3] [2 4] [3 5]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Y).
                                         b(X, Y) :- a(X, Z), p(Z, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 4] [4 5]})
                 (wcoj/query-by-name 'b)
                 (set)))))

  (t/testing "horn 3"
    (t/is (= #{[1 4] [2 5]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Y).
                                         b(X, Y) :- a(X, Z), p(Z, Y).
                                         c(X, Y) :- b(X, Z), p(Z, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 4] [4 5]})
                 (wcoj/query-by-name 'c)
                 (set)))))

  (t/testing "cartesian product"
    (t/is (= #{[1 1] [1 2] [1 3]
               [2 1] [2 2] [2 3]
               [3 1] [3 2] [3 3]}
             (-> (wcoj/execute-datalog '[h(X, Y) :- p(X), p(Y).])
                 (wcoj/assert-all 'p #{[1] [2] [3]})
                 (wcoj/query-by-name 'h)
                 (set)))))

  (t/testing "infix eq"
    (t/is (= #{[2]}
             (-> (wcoj/execute-datalog '[h(X) :- p(X), X = 2 .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'h)
                 (set)))))

  (t/testing "infix lt"
    (t/is (= #{[1] [2]}
             (-> (wcoj/execute-datalog '[h(X) :- p(X), X < 3 .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'h)
                 (set)))))

  (t/testing "infix le"
    (t/is (= #{[1] [2]}
             (-> (wcoj/execute-datalog '[h(X) :- p(X), X <= 2 .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'h)
                 (set)))))

  (t/testing "infix gt"
    (t/is (= #{[3] [4]}
             (-> (wcoj/execute-datalog '[h(X) :- p(X), X > 2 .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'h)
                 (set)))))

  (t/testing "infix ge"
    (t/is (= #{[3] [4]}
             (-> (wcoj/execute-datalog '[h(X) :- p(X), X >= 3 .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'h)
                 (set)))))

  (t/testing "infix ne"
    (t/is (= #{[1] [3] [4]}
             (-> (wcoj/execute-datalog '[h(X) :- p(X), X != 2 .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'h)
                 (set)))))

  ;; These are a bit odd as some parts dedupes streams atm.
  (t/testing "union 2"
    (t/is (= (set [[1 2] [2 3] [3 4] [3 4] [4 5] [4 5]])
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Y), X > 2 .
                                         a(X, Y) :- p(X, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 4] [4 5]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "union 3"
    (t/is (= (set [[1 2] [1 2] [2 3] [2 3] [3 4] [3 4] [4 5] [4 5]])
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Y), X > 2 .
                                         a(X, Y) :- p(X, Y), X < 3 .
                                         a(X, Y) :- p(X, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 4] [4 5]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "recursion 1"
    (t/is (= #{[1 2] [2 3] [1 3] [3 3] [3 2] [2 2]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Y).
                                         a(X, Y) :- a(X, Z), p(Z, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 2]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "recursion 2"
    (t/is (= #{[1 2] [2 3] [1 3] [3 4] [2 4] [1 4] [4 5] [3 5] [2 5] [1 5]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Y).
                                         a(X, Y) :- p(X, Z), a(Z, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [3 4] [4 5]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "recursion 3"
    (t/is (= #{[4 3] [2 2] [2 3] [2 5] [3 3] [5 4] [3 4] [4 2] [5 3] [5 2] [1 4]
               [1 3] [1 5] [5 5] [2 4] [4 5] [4 4] [1 2] [3 5] [3 2]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X, Y).
                                         a(X, Y) :- a(X, Z), p(Z, Y).])
                 (wcoj/assert-all 'p #{[1 2] [2 3] [2 5] [3 4] [4 2] [5 4]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "lang eq"
    (t/is (= #{[1 1] [2 2] [3 3] [4 4]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X), p(Y), X = Y .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "lang ne"
    (t/is (= #{[1 2] [2 1]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X), p(Y), X != Y .])
                 (wcoj/assert-all 'p #{[1] [2]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "lang lt"
    (t/is (= #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X), p(Y), X < Y .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "lang gt"
    (t/is (= #{[2 1] [3 1] [4 1] [3 2] [4 2] [4 3]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X), p(Y), X > Y .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "lang le"
    (t/is (= #{[1 1] [1 2] [1 3] [1 4] [2 2] [2 3] [2 4] [3 3] [3 4] [4 4]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X), p(Y), X <= Y .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'a)
                 (set)))))

  (t/testing "lang ge"
    (t/is (= #{[1 1] [2 1] [2 2] [3 1] [3 2] [3 3] [4 1] [4 2] [4 3] [4 4]}
             (-> (wcoj/execute-datalog '[a(X, Y) :- p(X), p(Y), X >= Y .])
                 (wcoj/assert-all 'p #{[1] [2] [3] [4]})
                 (wcoj/query-by-name 'a)
                 (set))))))
