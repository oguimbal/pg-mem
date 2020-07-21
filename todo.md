node-sql-parser
- create index on test(a+b); should NOT parse as a valid statement (require parenthesis)
- support parsing arrays
- support START TRANSACTION; COMMIT; ROLLBACK;
- support calling current_schema (both select * from current_schema and select * from current_schema() are working)... see #todo.md


- remove all replace() hacks in query.ts
- test "is true" vs "is not false" (the latter selects nulls) / same for "is false" vs "is not false"
- test that "a is true is false" returns nulls like "is not true"
      => is also equivalent to "a is true is not true"