<p align="center">
  <a href="https://npmjs.org/package/pg-mem"><img src="http://img.shields.io/npm/v/pg-mem.svg"></a>
  <a href="https://npmjs.org/package/pg-mem"><img src="https://img.shields.io/npm/dm/pg-mem.svg"></a>
  <a href="https://david-dm.org/oguimbal/pg-mem"><img src="https://david-dm.org/oguimbal/pg-mem.svg"></a>
  <img src="https://github.com/oguimbal/pg-mem/workflows/CI/badge.svg">
</p>

<p align="center">
  <img src="./.github/pg_mem.png" width="200">
</p>

 <h3 align="center">pg-mem is an experimental in-memory emulation of a postgres database.</h3>

<p align="center">
❤ It works both in Node or in the browser.
</p>

<p align="center">
⭐ this repo if you like this package, it helps to motivate me :)

</p>

<p align="center">
  👉 See it in action with <a href="https://oguimbal.github.io/pg-mem-playground/">pg-mem playground</a>
</p>

- [Usage](#-usage)
- [Features](#-features)
- [Libraries adapters](#-libraries-adapters)
- [Inspection](#-inspection)
- [Development](#-development)
- [FAQ](https://github.com/oguimbal/pg-mem/wiki/FAQ)

# 📐 Usage

## Using Node.js

As always, it starts with an:

```bash
npm i pg-mem --save
```

Then, assuming you're using something like webpack, if you're targeting a browser:

```typescript
import { newDb } from "pg-mem";

const db = newDb();
db.public.many(/* put some sql here */);
```

## Using Deno

Pretty straightforward :)

```typescript
import { newDb } from "https://deno.land/x/pg_mem/mod.ts";

const db = newDb();
db.public.many(/* put some sql here */);
```

## Only use the SQL syntax parser

❤ Head to the [pgsql-ast-parser](https://github.com/oguimbal/pgsql-ast-parser) repo

## ⚠ Disclaimer

The sql syntax parser is [home-made](https://github.com/oguimbal/pgsql-ast-parser). Which means that some features are not implemented, and will be considered as invalid syntaxes.

This lib is quite new, so forgive it if some obvious pg syntax is not supported !

... And open an issue if you feel like a feature should be implemented :)

Moreover, even if I wrote hundreds of tests, keep in mind that this implementation is a best effort to replicate PG.
Keep an eye on your query results if you perform complex queries.
Please file issues if some results seem incoherent with what should be returned.

Finally, I invite you to read the below section to have an idea of you can or cannot do.

# 🔍 Features

## Rollback to a previous state

`pg-mem` uses immutable data structures ([here](https://www.npmjs.com/package/immutable) and [here](https://www.npmjs.com/package/functional-red-black-tree)),
which means that you can have restore points for free!

This is super useful if you intend to use `pg-mem` to mock your database for unit tests.

You could:

1. Create your schema only once (which could be a heavy operation for a single unit test)
2. Insert test data which will be shared by all test
3. Create a restore point
4. Run your tests with the same db instance, executing a `backup.restore()` before each test (which instantly resets db to the state it has after creating the restore point)

Usage:

```typescript
const db = newDb();
db.public.none(`create table test(id text);
                insert into test values ('value');`);
// create a restore point & mess with data
const backup = db.backup();
db.public.none(`update test set id='new value';`);
// restore it !
backup.restore();
db.public.many(`select * from test`); // => {test: 'value'}
```

## Custom functions

You can declare custom functions like this:

```typescript
db.public.registerFunction({
  name: "say_hello",
  args: [DataType.text],
  returns: DataType.text,
  implementation: (x) => "hello " + x,
});
```

And then use them like in SQL `select say_hello('world')`.

Custom functions support overloading and variadic arguments.

⚠ However, the value you return is not type checked. It MUST correspond to the datatype you provided as 'returns' (it won't fail if not, but could lead to weird bugs).

## Custom types

Not all pg types are implemented in pg-mem.
That said, most of the types are often equivalent to other types, with a format validation. pg-mem provides a way to register such types.

For instance, lets say you'd like to register the MACADDR type, which is basically a string, with a format constraint.

You can register it like this:

```typescript
db.public.registerEquivalentType({
  name: "macaddr",
  // which type is it equivalent to (will be able to cast it from it)
  equivalentTo: DataType.text,
  isValid(val: string) {
    // check that it will be this format
    return isValidMacAddress(val);
  },
});
```

Doing so, you'll be able to do things such as:

```sql
SELECT '08:00:2b:01:02:03:04:05'::macaddr; -- WORKS
SELECT 'invalid'::macaddr; -- will throw a conversion error
```

If you feel your implementation of a type matches the standard, and would like to include it in pg-mem for others to enjoy it, please consider filing a pull request ! (tip: see the [INET](https://github.com/oguimbal/pg-mem/blob/master/src/datatypes/t-inet.ts) type implementation as an example, and the [pg_catalog index](https://github.com/oguimbal/pg-mem/blob/master/src/schema/pg-catalog/index.ts) where supported types are registered)

## Extensions

No native extension is implemented (pull requests are welcome), but you can define kind-of extensions like this:

```typescript
db.registerExtension("my-ext", (schema) => {
  // install your ext in 'schema'
  // ex:  schema.registerFunction(...)
});
```

Statements like `create extension "my-ext"` will then be supported.

# 📃 Libraries adapters

pg-mem provides handy shortcuts to create instances of popular libraries that will be bound to pg-mem instead of a real postgres db.

- pg-native
- node-postgres (pg)
- pg-promise (pgp)
- slonik
- typeorm
- knex
- kysely
- mikro-orm

[See the wiki for more details](https://github.com/oguimbal/pg-mem/wiki/Libraries-adapters)

# 💥 Inspection

## Intercept queries

If you would like to hook your database, and return ad-hoc results, you can do so like this:

```typescript
const db = newDb();

db.public.interceptQueries((sql) => {
  if (sql === "select * from whatever") {
    // intercept this statement, and return something custom:
    return [{ something: 42 }];
  }
  // proceed to actual SQL execution for other requests.
  return null;
});
```

## Inspect a table

You can manually inspect a table content using the `find()` method:

```typescript
for (const item of db.public.getTable<TItem>("mytable").find(itemTemplate)) {
  console.log(item);
}
```

## Manually insert items

If you'd like to insert items manually into a table, you can do this like that:

```typescript
db.public.getTable<TItem>('mytable').insert({ /* item to insert */ }))
```

## Subscribe to events

You can subscribe to some events, like:

```typescript
const db = newDb();

// called on each successful sql request
db.on("query", (sql) => {});
// called on each failed sql request
db.on("query-failed", (sql) => {});
// called on schema changes
db.on("schema-change", () => {});
// called when a CREATE EXTENSION schema is encountered.
db.on("create-extension", (ext) => {});
```

## Experimental events

`pg-mem` implements a basic support for indices.

These handlers are called when a request cannot be optimized using one of the created indices.

However, a real postgres instance will be much smarter to optimize its requests... so when `pg-mem` says "this request does not use an index", dont take my word for it.

```typescript
// called when a table is iterated entirely (ex: 'select * from data where notIndex=3' triggers it)
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
- [How to import my production schema in pg-mem ?](https://github.com/oguimbal/pg-mem/wiki/FAQ#-how-to-import-my-production-schema-in-pg-mem-) _TLDR: pg_dump with the right args_
- [Does pg-mem supports sql migrations ?](https://github.com/oguimbal/pg-mem/wiki/FAQ#-does-pg-mem-support-sql-migrations-scripts-) _TLDR: yes._
- [Does pg-mem supports plpgsql/other scripts/"create functions"/"do statements" ?](https://github.com/oguimbal/pg-mem/wiki/FAQ#-how-to-use-plpgsql-or-other-scripts-) _TLDR: kind of..._

Detailed answers [in the wiki](https://github.com/oguimbal/pg-mem/wiki/FAQ)

# ⚠️ Current limitations

- Materialized views are implemented as views (meaning that they are always up-to-date, without needing them to refresh)
- Indices implementations are basic
- No support for timezones
- All number-like types are all handled as javascript numbers, meaning that types like `numeric(x,y)` could not behave as expected.

# 🐜 Development

Pull requests are welcome :)

Unit tests are ran using [Bun](https://bun.sh/), which you will have to install to run tests.

## Run all tests

```bash
bun test
```

## Debug a test

Using vscode:

1. Add a `.only` on the test you'd like to debug
2. Just hit F5 (or execute via the debugger tab), which should launch your test with debugger attached
