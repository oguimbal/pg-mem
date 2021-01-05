# What to do ?

There are plenty of postgres features that are not implemted yet.

1) Head to  [pg-mem playground](https://oguimbal.github.io/pg-mem-playground/)
2) Find some syntax or pg feature that throws an error, but which shouldnt
3) If it's a parsing exception, add parser unit test [in pgsql-sql-parser](https://github.com/oguimbal) like [these ones][in pgsql-sql-parser](https://github.com/oguimbal/src/pgsql-ast-parser/src/syntax/delete.spec.ts) for instance
4) If parsing is OK but the statement you're executing do not behave as expected, add a runner unit test [like those ones](/src/tests/delete.queries.spec.ts)
5) Fix unit tests :)


Dont hesitate to create issues to ask questions, guidance or help.


# Some long-term todos:


It does not (yet) support (this is kind-of a todo list):
- [ ] Gin Indices
- [ ] Cartesian Joins
- [ ] Most of the pg functions are not implemented - ask for them, [they're easy to implement](src/functions) !
- [ ] Some [aggregations](src/transforms/aggregation.ts) are to be implemented (avg, count, ...) - easy job, but not yet done.
- [ ] Stored procedures
- [ ] Lots of small and not so small things (collate, timezones, tsqueries, custom types ...)
- [ ] Introspection schema (it is faked - i.e. some table exist, but are empty - so Typeorm can inspect an introspect an empty db & create tables)
- [ ] Concurrent transaction commit
- [ ] Collation (see #collation tag in code if you want to implement it)
