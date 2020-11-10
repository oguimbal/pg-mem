# What to do ?

There are plenty of postgres features that are not implemted yet.

1) Head to  [pg-mem playground](https://oguimbal.github.io/pg-mem-playground/)
2) Find some syntax or pg feature that throws an error, but which shouldnt
3) If it's a parsing exception, add parser unit test [in pgsql-sql-parser](https://github.com/oguimbal) like [these ones][in pgsql-sql-parser](https://github.com/oguimbal/src/pgsql-ast-parser/src/syntax/delete.spec.ts) for instance
4) If parsing is OK but the statement you're executing do not behave as expected, add a runner unit test [like those ones](/src/tests/delete.queries.spec.ts)
5) Fix unit tests :)


Dont hesitate to create issues to ask questions, guidance or help.
