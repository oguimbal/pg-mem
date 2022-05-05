# What to do ?

There are plenty of postgres features that are not implemted yet.

1. Head to [pg-mem playground](https://oguimbal.github.io/pg-mem-playground/)
2. Find some syntax or pg feature that throws an error, but which shouldnt
3. If it's a parsing exception, add parser unit test in [pgsql-ast-parser](https://github.com/oguimbal/pgsql-ast-parser). Take [these ones](https://github.com/oguimbal/pgsql-ast-parser/blob/master/src/syntax/delete.spec.ts) as reference.
4. If parsing is OK but the statement you're executing do not behave as expected, add a runner unit test [like those ones](/src/tests/delete.queries.spec.ts)
5. Fix unit tests :)

Dont hesitate to create issues to ask questions, guidance or help.

# how to implement a new statement

If implementing an unsupported SQL statement may somtimes be hard, it's actually easy to explain how to get started.

## A) Implement its parser

[pgsql-ast-parser](https://github.com/oguimbal/pgsql-ast-parser) uses [Nearley](https://nearley.js.org/) (all .ne files) to build a syntax parser.

1. Look very hard at the statement syntax (for instance, here is [create sequence](https://www.postgresql.org/docs/current/sql-createsequence.html))
2. Add the target statement type in [pgsql-ast-parser AST](https://github.com/oguimbal/pgsql-ast-parser/blob/7358e4a1fe0b3fe79ae047a936673745cc17b5f5/src/syntax/ast.ts#L6) based on what I understood.
3. Implement unit tests I want to pass (ex with [create sequence UTs](https://github.com/oguimbal/pgsql-ast-parser/blob/master/src/syntax/sequence.spec.ts))
4. Implement its syntax in a new nearley file (example with the [create sequence](https://github.com/oguimbal/pgsql-ast-parser/blob/master/src/syntax/sequence.ne) statement) and reference it in [main.ne](https://github.com/oguimbal/pgsql-ast-parser/blob/7358e4a1fe0b3fe79ae047a936673745cc17b5f5/src/syntax/main.ne#L56)
5. Work on it until unit tests are all failing, but with an error which is not a parsing error
6. Add an entry for the new statement in `IAstPartialMapper` here and add its default visitor implem like [here](https://github.com/oguimbal/pgsql-ast-parser/blob/7358e4a1fe0b3fe79ae047a936673745cc17b5f5/src/ast-mapper.ts#L308-L313) and [here](https://github.com/oguimbal/pgsql-ast-parser/blob/7358e4a1fe0b3fe79ae047a936673745cc17b5f5/src/ast-mapper.ts#L223-L224) until compiler stops screaming
7. Add the "toSql" implementation of the new statement like [here](https://github.com/oguimbal/pgsql-ast-parser/blob/7358e4a1fe0b3fe79ae047a936673745cc17b5f5/src/to-sql.ts#L432-L443) (until compiler stops)
8. Check that ALL unit tests are green (not only the new ones)
9. Release pgsql-ast-parser

NB: very seldom [the lexer](https://github.com/oguimbal/pgsql-ast-parser/blob/master/src/lexer.ts) will need to be updated in order to implement the required feature.

## B) Executor

1. install pgsql-ast-parser@latest
2. Write unit tests (create sequence UTs are [here](https://github.com/oguimbal/pg-mem/blob/master/src/tests/sequence.spec.ts), for instance)
3. Implement statement... that's the less codified thing, but it starts by adding a new case [in this switch](https://github.com/oguimbal/pg-mem/blob/4b8a36d53e481916ba4291e045ac6edae8682b31/src/schema.ts#L124-L219) for new statements (which should fail to compile with the newly installed pgsql-ast-parser), or fix compilation errors & unit tests for other amendments to the parser.
4. Check that ALL unit tests are OK
5. Release 🎉

# Some long-term todos:

It does not (yet) support (this is kind-of a todo list):

- [ ] Optimize comma-separated cross-joins by picking join conditions in the where statement & using corresonding indinces (currently enumerating the whole cross-join, which is painful)
- [ ] Gin Indices
- [ ] Cartesian Joins
- [ ] Most of the pg functions are not implemented - ask for them, [they're easy to implement](src/functions) !
- [ ] Some [aggregations](src/transforms/aggregation.ts) are to be implemented (avg, count, ...) - easy job, but not yet done.
- [ ] Stored procedures
- [ ] Lots of small and not so small things (collate, timezones, tsqueries, custom types ...)
- [ ] Introspection schema (it is faked - i.e. some table exist, but are empty - so Typeorm can inspect an introspect an empty db & create tables)
- [ ] Concurrent transaction commit
- [ ] Collation (see #collation tag in code if you want to implement it)
