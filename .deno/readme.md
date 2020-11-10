# What is it ?

`pg-mem` is an experimental in-memory emulation of a postgres database.

It works both in node or in browser.

## See it in action with [pg-mem playground](https://oguimbal.github.io/pg-mem-playground/)

## DISCLAIMER

The sql syntax parser is home-made. Which means that some features are not implemented, and will be considered as invalid syntaxes.

This lib is quite new, so forgive it if some obivious pg syntax is not supported !

... And open an issue if you feel like a feature should be implemented :)

Moreover, even if I wrote hundreds of tests, keep in mind that this implementation is a best effort to replicate PG.
Keep an eye on your query results if you perform complex queries.
Please file issues if some results seem incoherent with what should be returned.

Finally, I invite you to read the below section to have an idea of you can or cannot do.


# Supported features

It supports:
- [x] Indices, somewhat (on "simple" requests)
- [x] Basic data types (json, dates, ...)
- [x] Joins, group bys, ...
- [x] Easy wrapper creator for [Typeorm](https://github.com/typeorm/typeorm), [pg-promise (pgp)](https://github.com/vitaly-t/pg-promise), [node-postgres (pg)](https://github.com/brianc/node-postgres), [pg-native](https://github.com/brianc/node-pg-native)
- [x] Transactions (only one of multiple concurrent transactions can be commited, though)


It does not (yet) support (this is kind-of a todo list):
- [ ] Gin Indices
- [ ] Cartesian Joins
- [ ] Most of the pg functions are not implemented - ask for them, [they're easy to implement](src/functions.ts) !
- [ ] Some [aggregations](src/transforms/aggregation.ts) are to be implemented (avg, count, ...) - easy job, but not yet done.
- [ ] Stored procedures
- [ ] Lots of small and not so small things (collate, timezones, tsqueries, custom types ...)
- [ ] Introspection schema (it is faked - i.e. some table exist, but are empty - so Typeorm can inspect an introspect an empty db & create tables)
- [ ] Concurrent transaction commit

... PRs are open :)

# Usage


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

# Features

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

## pg-native

You can ask `pg-mem` to get you an object wich implements the same behaviour as [pg-native](https://github.com/brianc/node-pg-native).


```typescript
// instead of
import Client from 'pg-native';

// use:
import {newDb} from 'pg-mem';
const Client = newDb.adapters.createPgNative();
```


## node-postgres (pg)

You can use `pg-mem` to get a memory version of the [node-postgres (pg)](https://github.com/brianc/node-postgres) module.

```typescript
// instead of
import {Client} from 'pg';

// use:
import {newDb} from 'pg-mem';
const {Client} = newDb.adapters.createPg();
```


## pg-promise (pgp)

You can ask `pg-mem` to get you a [pg-promise](https://github.com/vitaly-t/pg-promise) instance bound to this db.

Given that pg-promise [does not provide](https://github.com/vitaly-t/pg-promise/issues/743) any way to be hooked, [I had to fork it](https://github.com/oguimbal/pg-promise).
You must install this fork in order to use this  (not necessarily use it in production):

```bash
npm i @oguimbal/pg-promise -D
```

Then:

```typescript
// instead of
import pgp from 'pg-promise';
const pg = pgp(opts)

// use:
import {newDb} from 'pg-mem';
const pg = await newDb.adapters.createPgPromise();

// then use it like you would with pg-promise
await pg.connect();
```


## slonik

You can use `pg-mem` to get a memory version of a [slonik](https://github.com/gajus/slonik) pool.

```typescript
// instead of
import {createPool} from 'slonik';
const pool = createPool(/* args */);

// use:
import {newDb} from 'pg-mem';
const pool = newDb.adapters.createSlonik();
```


## Typeorm

You can use `pg-mem` as a backend database for [Typeorm](https://github.com/typeorm/typeorm), [node-postgres (pg)](https://github.com/brianc/node-postgres).

Usage:
```typescript
const db = newDb();
const connection = await db.adapters.createTypeormConnection({
    type: 'postgres',
    entities: [/* your entities here ! */]
})

// create schema
await connection.synchronize();

// => you now can user your typeorm connection !
```

See detailed examples [here](samples/typeorm/simple.ts) and [here](samples/typeorm/joins.ts).

See restore points (section above) to avoid running schema creation  (`.synchronize()`) on each test.

__NB: Restore points only work if the schema has not been changed after the restore point has been created__

note: You must install `typeorm` module first.

# Inspection

## Subscriptions
You can subscribe to some events, like:

```typescript
const db = newDb();

// called on each successful sql request
db.on('query', sql => {  });
// called on each failed sql request
db.on('query-failed', sql => { });
// called on schema changes
db.on('schema-change', () => {});
```

## Experimental subscriptions

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

# Development

Pull requests are welcome :)

To start hacking this lib, you'll have to:
- Use vscode
- Install [mocha test explorer with HMR support](https://marketplace.visualstudio.com/items?itemName=oguimbal.vscode-mocha-test-adapter) extension
- `npm start`
- Reload unit tests in vscode

... once done, tests should appear. HMR is on, which means that changes in your code are instantly propagated to unit tests.
This allows for ultra fast development cycles (running tests takes less than 1 sec).

To debug tests: Just hit "run" (F5, or whatever)... vscode should attach the mocha worker. Then run the test you want to debug.
