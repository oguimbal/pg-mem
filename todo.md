node-sql-parser
- create index on test(a+b); should NOT parse as a valid statement (require parenthesis)
- support parsing arrays
- support START TRANSACTION; COMMIT; ROLLBACK;
- support calling current_schema (both select * from current_schema and select * from current_schema() are working)... see #todo.md


- remove all replace() hacks in query.ts