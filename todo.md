

- remove all replace() hacks in query.ts
- test "is true" vs "is not false" (the latter selects nulls) / same for "is false" vs "is not false"
- test that "a is true is false" returns nulls like "is not true"
      => is also equivalent to "a is true is not true"

- support cartesian join syntax:" select * from a,b" ... 👉 this syntax must detect a join when a where condition is specified.


- UT: check that throws ambiguous column: "select x.a from (select val1 as a, val2 as a from tbl) x;"
- Handle "insert into tbl select * from otherTbl"


- "auto-create" tables mode (which guess/adapt their schema on insert)
- ORDER BY
- LIMIT