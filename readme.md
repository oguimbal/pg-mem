
<p align="center">
  <a href="https://npmjs.org/package/pg-mem)"><img src="http://img.shields.io/npm/v/pg-mem.svg"></a>
  <a href="https://npmjs.org/package/pg-mem"><img src="https://img.shields.io/npm/dm/pg-mem.svg"></a>
  <a href="https://david-dm.org/oguimbal/pg-mem"><img src="https://david-dm.org/oguimbal/knex.svg"></a>
  <img src="https://github.com/oguimbal/pg-mem/workflows/CI/badge.svg">
</p>

> `pg-mem` is an experimental in-memory emulation of a postgres database.

❤ It works both in node or in browser.

⭐ this repo if you like this package, it helps to motivate me :)

👉 See it in action with [pg-mem playground](https://oguimbal.github.io/pg-mem-playground/)


* [Usage](#-usage)
* [Features](#-features)
* [Libraries adapters](#-libraries-adapters)
* [Inspection](#inspection)
* [FAQ](#-faq)
* [Supported features](#-supported-features)
* [Development](#-development)


# 📐 Usage


## Using NodeJS
As always, it stats with an:

```bash
npm i pg-mem --save
```

Then, assuming you're using something like Webpack if you're targetting a browser:

```typescript
import { newDb } from 'pg-mem';

const db = newDb();
db.public.many(/* put some sql here */)
```

## Using Deno

Pretty straightforward :)

```typescript
import { newDb } from 'https://deno.land/x/pg_mem/mod.ts';

const db = newDb();
db.public.many(/* put some sql here */)
```


## Only use the SQL syntax parser

❤ Head to the [pgsql-ast-parser](https://github.com/oguimbal/pgsql-ast-parser) repo


## ⚠ Disclaimer

The sql syntax parser is [home-made](https://github.com/oguimbal/pgsql-ast-parser). Which means that some features are not implemented, and will be considered as invalid syntaxes.

This lib is quite new, so forgive it if some obivious pg syntax is not supported !

... And open an issue if you feel like a feature should be implemented :)

Moreover, even if I wrote hundreds of tests, keep in mind that this implementation is a best effort to replicate PG.
Keep an eye on your query results if you perform complex queries.
Please file issues if some results seem incoherent with what should be returned.

Finally, I invite you to read the below section to have an idea of you can or cannot do.

# 🔍 Features

## Rollback to a previous state

`pg-mem` uses immutable data structures ([here](https://www.npmjs.com/package/immutable) and [here](https://www.npmjs.com/package/functional-red-black-tree)),
which means that you can have restore points for free !

This is super useful if you indend to use `pg-mem` to mock your database for unit tests.
You could:

1) Create your schema only once (which could be an heavy operation for a single unit test)
2) Insert test data which will be shared by all test
2) Create a restore point
3) Run your tests with the same db instance, executing a `backup.restore()` before each test (which instantly resets db to the state it has after creating the restore point)

Usage:
```typescript
const db = newDb();
db.public.none(`create table test(id text);
                insert into test values ('value');`);
// create a restore point & mess with data
const backup = db.backup();
db.public.none(`update test set id='new value';`)
// restore it !
backup.restore();
db.public.many(`select * from test`) // => {test: 'value'}
```


## Custom functions

You can declare custom functions like this:

```typescript
db.public.registerFunction({
            name: 'say_hello',
            args: [DataType.text],
            returns: DataType.text,
            implementation: x => 'hello ' + x,
        })
```

And then use them like in SQL `select say_hello('world')`.

Custom functions support overloading and variadic arguments.

⚠ However, the value you return is not type checked. It MUST correspond to the datatype you provided as 'returns' (wont fail if not, but could lead to weird bugs).


## Extensions

No native extension is implemented (pull requests are welcome), but you can define kind-of extensions like this:

```typescript

db.registerExtension('my-ext', schema => {
    // install your ext in 'schema'
    // ex:  schema.registerFunction(...)
});
```

Statements like `create extension "my-ext"` will then be supported.

# 📃 Libraries adapters

pg-mem provides handy shortcuts to create instances of popuplar libraries that will be bound to pg-mem instead of a real postgres db.

- pg-native
- node-postgres (pg)
- pg-promise (pgp)
- slonik
- typeorm
- knex

[See the wiki for more details](https://github.com/oguimbal/pg-mem/wiki/Libraries-adapters)

# Inspection

## 💥 Subscribe to events
You can subscribe to some events, like:

```typescript
const db = newDb();

// called on each successful sql request
db.on('query', sql => {  });
// called on each failed sql request
db.on('query-failed', sql => { });
// called on schema changes
db.on('schema-change', () => {});
// called when a CREATE EXTENSION schema is encountered.
db.on('create-extension', ext => {});
```

## Experimental events

`pg-mem` implements a basic support for indices.

These handlers are called when a request cannot be optimized using one of the created indices.

However, a real postgres instance will be much smarter to optimize its requests... so when `pg-mem` says "this request does not use an index", dont take my word for it.

```typescript
// called when a table is iterated entierly (ex: 'select * from data where notIndex=3' triggers it)
db.on('seq-scan', () => {});

// same, but on a specific table
db.getTable('myTable').on('seq-scan', () = {});

// will be called if pg-mem did not find any way to optimize a join
// (which leads to a O(n*m) lookup with the current implementation)
db.on('catastrophic-join-optimization', () => {});
```


# 🙋‍♂️ FAQ

- [Why this instead of Docker ?](https://github.com/oguimbal/pg-mem/wiki/FAQ#-why-use-pg-mem-instead-of-an-instance-of-postgres-in-docker-) _TLDR : It's faster. Docker is overkill._
- [What if I need an extension like uuid-ossp ?](https://github.com/oguimbal/pg-mem/wiki/FAQ#-what-if-i-need-an-extension-like-uuid-ossp-) _TLDR: You can mock those_
- [How to import my production schema in pg-mem ?](https://github.com/oguimbal/pg-mem/wiki/FAQ#-how-to-import-my-production-schema-in-pg-mem-) _TLDR: pg\_dump with the right args_
- [Does pg-mem support sql migrations ?](https://github.com/oguimbal/pg-mem/wiki/FAQ#-does-pg-mem-support-sql-migrations-scripts-) _TLDR: yes._

Detailed answers [in the wiki](https://github.com/oguimbal/pg-mem/wiki/FAQ)

# 📃 Supported features

It supports:
- [x] Indices, somewhat (on "simple" requests)
- [x] Basic data types (json, dates, ...)
- [x] Joins, group bys, ...
- [x] Easy wrapper creator for [Typeorm](https://github.com/typeorm/typeorm), [pg-promise (pgp)](https://github.com/vitaly-t/pg-promise), [node-postgres (pg)](https://github.com/brianc/node-postgres), [pg-native](https://github.com/brianc/node-pg-native)
- [x] Transactions (only one of multiple concurrent transactions can be commited, though)


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

... PRs are open :)

# 🐜 Development

Pull requests are welcome :)

To start hacking this lib, you'll have to:
- Use vscode
- Install [mocha test explorer with HMR support](https://marketplace.visualstudio.com/items?itemName=oguimbal.vscode-mocha-test-adapter) extension
- `npm start`
- Reload unit tests in vscode

... once done, tests should appear. HMR is on, which means that changes in your code are instantly propagated to unit tests.
This allows for ultra fast development cycles (running tests takes less than 1 sec).

To debug tests: Just hit "run" (F5, or whatever)... vscode should attach the mocha worker. Then run the test you want to debug.

Alternatively, you could just run `npm run test` wihtout installing anything, but this is a bit long.
