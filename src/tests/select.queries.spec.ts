import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { Types } from '../datatypes';
import { _IDb } from '../interfaces-private';
import { SelectFromStatement, SelectStatement } from 'pgsql-ast-parser';
import { buildValue } from '../parser/expression-builder';
import { parseSql } from '../parser/parse-cache';
import { withSelection } from '../parser/context';
import { DataType, QueryResult } from '../interfaces';
import { expectQueryError } from './test-utils';
import { cleanResults } from '../execution/clean-results';

describe('Selections', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let query: (str: string) => QueryResult;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        query = db.public.query.bind(db.public);
    });

    function simpleDb() {
        db.public.declareTable({
            name: 'data',
            fields: [{
                name: 'id',
                type: Types.text(),
                constraints: [{ type: 'primary key' }],
            }, {
                name: 'str',
                type: Types.text(),
            }, {
                name: 'otherstr',
                type: Types.text(),
            }],
        });
        return db;
    }

    function stuff() {
        none(`create table test(txt text, val integer);
        insert into test values ('A', 999);
        insert into test values ('A', 0);
        insert into test values ('A', 1);
        insert into test values ('B', 2);
        insert into test values ('C', 3);`)
    }



    it('can select nothing', () => {
        // yea... thats a valid query. Try it oO'
        expect(many(`select;`))
            .toEqual([{}]);
    });

    it('can use transformations', () => {
        stuff();
        expect(many(`select * from (select val as xx from test where txt = 'A') x where x.xx >= 1`))
            .toEqual([{ xx: 999 }, { xx: 1 }]);
    });

    it('executes the right plan on transformations', () => {
        stuff();
        const plan = (db as _IDb).public.explainSelect(`select * from (select val as valAlias from test where txt = 'A') x where x.valAlias >= 1`);
        // assert.deepEqual(plan, {} as any);
        expect(plan).toEqual({
            id: 1,
            _: 'seqFilter',
            filtered: {
                id: 2,
                _: 'map',
                select: [{
                    what: {
                        col: 'val',
                        on: 'test',
                    },
                    as: 'valalias',
                }],
                of: {
                    id: 3,
                    _: 'seqFilter',
                    filtered: {
                        _: 'table',
                        table: 'test',
                    }
                }
            }
        });
    })


    it('can use an expression on a transformed selection', () => {
        stuff();
        // preventSeqScan(db);
        expect(many(`select *, lower(txtx) as v from (select val as valx, txt as txtx from test where val >= 1) x where lower(x.txtx) = 'a'`))
            .toEqual([{ txtx: 'A', valx: 999, v: 'a' }, { txtx: 'A', valx: 1, v: 'a' }]);
    });


    it('selects case whithout condition', () => {
        simpleDb();
        expect(many(`insert into data(id, str) values ('id1', 'SOME STRING'), ('id2', 'other string'), ('id3', 'Some String');
            select case when id='id1' then 'one ' || str else 'something else' end as x from data`))
            .toEqual([{ x: 'one SOME STRING' }, { x: 'something else' }, { x: 'something else' }]);
    })

    it('selects case with disparate types results', () => {
        simpleDb();
        expect(many(`select case when 2 > 1 then 1.5 when 2 < 1 then 1 end as x`))
            .toEqual([{ x: 1.5 }]);
    })

    it('does not support select * on dual', () => {
        expectQueryError(() => many(`select *`));
    });

    it('supports concat operator', () => {
        expect(many(`select 'a' || 'b' as x`))
            .toEqual([{ x: 'ab' }]);
    });

    it('has an index', () => {
        simpleDb();
        const [{ where }] = parseSql(`select * from data where id='x'`) as SelectFromStatement[];
        if (!where || where.type !== 'binary') {
            expect('Should be a binary').toBe('');
        }
        const data = db.getTable('data').selection;
        const built = withSelection(data, () => buildValue((where as any).left));
        expect(built.index).toBeTruthy();
    });


    it('detects ambiguous column selections on aliases', () => {
        // same-name columns not supported...if supported, must continue to throw when doing this:
        expectQueryError(() => none(`create table data(id text primary key, str text);
            select x.a from (select id as a, str as a from data) x;`), /column reference "a" is ambiguous/);
    });



    it('can select for update', () => {
        expect(many(`create table test (a text);
                    insert into test(a) values ('v');
                    select * from test for update`))
            .toEqual([
                { a: 'v' },
            ])
    })

    it('can order by asc with nulls first', () => {
        // see https://github.com/oguimbal/pg-mem/issues/133
        expect(many(`create table test(val text);
            insert into test values ('b'), ('a'), (null);
            select t.val as value from test t order by t.val asc nulls first`))
            .toEqual([
                { value: null }
                , { value: 'a' }
                , { value: 'b' }
            ]);
    });

    it('can select from values', () => {
        expect(many(`select * from (values (1, 'one'), (2, 'two')) as  tbl (num, str);`))
            .toEqual([
                { num: 1, str: 'one' },
                { num: 2, str: 'two' },
            ])
    })


    it('can select qualified from values', () => {
        expect(many(`select tbl.num, tbl.str from (values (1, 'one'), (2, 'two')) as  tbl (num, str);`))
            .toEqual([
                { num: 1, str: 'one' },
                { num: 2, str: 'two' },
            ])
    })

    it('can filter from values', () => {
        expect(many(`select * from (values (1, 'one'), (2, 'two')) as  tbl (num, str)  where tbl.num>1;`))
            .toEqual([
                { num: 2, str: 'two' },
            ])
    });

    it('can select from function with selection', () => {
        expect(many(`select a,b from concat('a') as a join concat('a') as b on b=a`))
            .toEqual([{ a: 'a', b: 'a' }])
    })

    it('can select from function without alias', () => {
        expect(many(`select * from concat('a') as a join concat('a') as b on b=a`))
            .toEqual([{ a: 'a', b: 'a' }])
    })

    it('can select from function with alias', () => {
        expect(many(`select * from concat('a') as a join concat('a') as b on a.a=b.b`))
            .toEqual([{ a: 'a', b: 'a' }])
    })


    it('can select record when not aliased', () => {
        expect(many(`create table test (a text, b text);
                    insert into test values ('a', 'b');
                    select test from test;`))
            .toEqual([{ test: { a: 'a', b: 'b' } }]);
    })

    it.skip('can select record when aliased', () => {
        expect(many(`create table test (a text, b text);
                    insert into test values ('a', 'b');
                    select v from test as v;`))
            .toEqual([{ v: { a: 'a', b: 'b' } }]);
    })

    it.skip('does not select parent alias record', () => {
        many(`create table test (a text, b text);
                    insert into test values ('a', 'b')`);
        expectQueryError(() => many(`select test from test as v`), /does not exist/);
    })

    it.skip('does not select record when scoped', () => {
        many(`create table test (a text, b text);
                    insert into test values ('a', 'b')`);
        expectQueryError(() => many(`select test.test from test`), /does not exist/);
    })

    it('can alias record selection', () => {
        expect(many(`create table test (a text, b text);
                    insert into test values ('a', 'b');
                    select test as v from test;`))
            .toEqual([{ v: { a: 'a', b: 'b' } }]);
    })


    it('prefers column seleciton over record selection', () => {
        expect(many(`create table test (test text, b text);
                    insert into test values ('a', 'b');
                    select test from test;`))
            .toEqual([{ test: 'a' }]);
    })


    it('does not leak null symbols (bugfix)', () => {
        expect(many(`
        create table test(data jsonb);
        insert into test values ('{"value": null}'), ('{"value":[null]}'), (null);
        select * from test;
        `)).toEqual([
            { data: { value: null } },
            { data: { value: [null] } },
            { data: null },
        ])
    })



    it('can select raw values', () => {
        expect(many(`values (1), (2)`))
            .toEqual([
                { column1: 1 },
                { column1: 2 },
            ]);
        expect(many(`values (1, 2)`))
            .toEqual([
                { column1: 1, column2: 2 },
            ]);
    });

    it('cannot select raw values when not same length', () => {
        expectQueryError(() => many(`values (1, 2), (3)`), /VALUES lists must all be the same length/);
    })


    it('can map column names 1', () => {
        expect(many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select * from test as xxx(a,b);
        `))
            .toEqual([
                { a: 'id', b: 'name', value: 'value' },
            ]);
    });


    it('can map column names 2', () => {
        expect(many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select * from test newalias(a,b);
        `))
            .toEqual([
                { a: 'id', b: 'name', value: 'value' },
            ]);
    });

    it('map column names with conflict', () => {
        expect(many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select * from test newalias(a,value);
        `))
            .toEqual([
                { a: 'id', value: 'name', value1: 'value' },
            ]);
    });

    it('cannot map column names when too many names specified', () => {
        expectQueryError(() => many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select * from test newalias(a,b,c,d);
        `), /table "test" has 3 columns available but 4 columns specified/);
    });

    it('cannot select non-existent column', () => {
        expectQueryError(() => many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select bogus from test;
        `), /column "bogus" does not exist/);
    });

    it('cannot select non-existent column with ORDER BY', () => {
        expectQueryError(() => many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select bogus from test ORDER BY value;
        `), /column "bogus" does not exist/);
    });

    it('cannot use default in expression', () => {
        expectQueryError(() => many(`values (1, default)`), /DEFAULT is not allowed in this context/);
    });

    it('bugfix on AND & OR operators', () => {
        // cf https://github.com/oguimbal/pg-mem/issues/201

        expect(many(`CREATE TABLE test(id serial, a int, b int);
        INSERT INTO test(a,b) VALUES (1, 2), (3, 4);

        SELECT *
            FROM   test
            WHERE id > 0 AND (
                (a = 1 AND b = 2)
            OR (a = 3 AND b = 4)
            );`))
            .toEqual([
                { id: 1, a: 1, b: 2 },
                { id: 2, a: 3, b: 4 },
            ]);
    })

    it('reports names and types of output columns in QueryResult.fields', () => {
        stuff();
        expect(cleanResults(query(`
        SELECT
            *, lower(txtx) AS v, valx + 1, valx + 2
        FROM (
            SELECT
                val AS valx,
                txt as txtx
            FROM test
            WHERE val >= 1
        ) x
        WHERE lower(x.txtx) = 'a'`))).toEqual({
            command: 'SELECT',
            rows: [
                { txtx: 'A', valx: 999, v: 'a', column: 1000, column1: 1001 },
                { txtx: 'A', valx: 1, v: 'a', column: 2, column1: 3 },
            ],
            fields: [
                { name: 'valx', type: DataType.integer, typeId: 23 },
                { name: 'txtx', type: DataType.text, typeId: 25 },
                { name: 'v', type: DataType.text, typeId: 25 },
                { name: 'column', type: DataType.integer, typeId: 23 },
                { name: 'column1', type: DataType.integer, typeId: 23 },
            ],
            location: {
                start: 0,
                end: 0,
            },
            rowCount: 2,
        })
    })

    it('can select fully qualified column names', () => {
        stuff();
        expect(many(`SELECT "public"."test"."val" FROM "public"."test"`))
            .toEqual([{ val: 999 }, { val: 0 }, { val: 1 }, { val: 2 }, { val: 3 }]);
    });
});