

- test "is true" vs "is not false" (the latter selects nulls) / same for "is false" vs "is not false"
- test that "a is true is false" returns nulls like "is not true"
      => is also equivalent to "a is true is not true"
- Handle "insert into tbl select * from otherTbl"
- "auto-create" tables mode (which guess/adapt their schema on insert)
- aggregation functions (avg...)
- better coverage


=== funky features
- exclusion constraints (?) - if implemented, beware of "insert xx on constraint yy"
- support cartesian join syntax:" select * from a,b" ... 👉 this syntax must detect a join when a where condition is specified.