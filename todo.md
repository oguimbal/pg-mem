

- remove all replace() hacks in query.ts
- test "is true" vs "is not false" (the latter selects nulls) / same for "is false" vs "is not false"
- test that "a is true is false" returns nulls like "is not true"
      => is also equivalent to "a is true is not true"

- support cartesian join syntax:" select * from a,b" ... 👉 this syntax must detect a join when a where condition is specified.

- UT: "select * from tbl where id=null"  => MUST RETURN NOTHING ! ... with or without indexes.
- UT: "select * from tbl where id is null"  => MUST RETURN NULL VALUES ! ... with or without indexes
- UT: check that throws ambiguous column: "select x.a from (select val1 as a, val2 as a from tbl) x;"
- Handle "insert into tbl select * from otherTbl"

- Review & refactor all .hasItem() & unit test them.

- "auto-create" tables mode (which guess/adapt their schema on insert)
- ORDER BY
- LIMIT
- DELETE