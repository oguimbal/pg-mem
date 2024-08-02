import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { expectQueryError, preventSeqScan } from './test-utils';
import { _IDb } from '../interfaces-private';
import moment from 'moment';

describe('Operators', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
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

    it('+ on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (1, 2);
                            select a+b as res from test`);
        expect(result.map(x => x.res)).toEqual([3]);
    });

    it('date + interval', () => {
        const result = many(`select  interval '1 day' + now()::date as dt`);
        const dt = result[0]?.dt
        expect(dt).toBeInstanceOf(Date);
        expect(dt.toString()).toBe(moment.utc().startOf('day').add(1, 'day').toDate().toString());
    });

    it('date - date', () => {
        const result = many(`select '2020-01-02'::date - '2020-01-01'::date as dt`);
        expect(result[0]?.dt).toBe(1);
        const result2 = many(`select '2020-01-03'::date - '2020-01-01'::date as dt`);
        expect(result2[0]?.dt).toBe(2);
        const result3 = many(`select '2022-01-01'::date - '2020-01-01'::date as dt`);
        expect(result3[0]?.dt).toBe(731);

        const nullResult1 = many(`select '2020-01-03'::date - null::date as dt`);
        expect(nullResult1[0]?.dt).toBe(null);

        const nullResult2 = many(`select null::date - '2020-01-03'::date as dt`);
        expect(nullResult2[0]?.dt).toBe(null);

        const nullResult3 = many(`select null::date - null::date as dt`);
        expect(nullResult3[0]?.dt).toBe(null);
    });

    it('timestamp + interval', () => {
        const result = many(`select now() + interval '1 day' as dt`);
        const dt = result[0]?.dt
        expect(dt).toBeInstanceOf(Date);
        expect(moment(dt).startOf('second').toISOString()).toBe(moment.utc().startOf('second').add(1, 'day').toISOString());
    });


    it('interval + timestamp', () => {
        const result = many(`select interval '1 day' + now() as dt`);
        const dt = result[0]?.dt
        expect(dt).toBeInstanceOf(Date);
        expect(moment(dt).startOf('second').toISOString()).toBe(moment.utc().startOf('second').add(1, 'day').toISOString());
    });

    it('timestamp - interval', () => {
        const result = many(`select now() - interval '1 day' as dt`);
        const dt = result[0]?.dt
        expect(dt).toBeInstanceOf(Date);
        expect(moment(dt).startOf('second').toISOString()).toBe(moment.utc().startOf('second').add(-1, 'day').toISOString());
    });

    it('- on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 1);
                            select a-b as res from test`);
        expect(result.map(x => x.res)).toEqual([1]);
    });

    it('/ on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (17, 10);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).toEqual([1]); // trunc is used on divisions
    });

    it('/ on neg ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (-17, 10);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).toEqual([-1]); // trunc is used on divisions
    });

    it('/ on int literals', () => {
        expect(one(`select 17/10 as res`)).toEqual({ res: 1 });
    })

    it('/ on float literals on second arg', () => {
        expect(one(`select 17/10.0 as res`)).toEqual({ res: 1.7 });
    });

    it('/ on float literals on first arg arg', () => {
        expect(one(`select 18.0/10 as res`)).toEqual({ res: 1.8 });
    })

    it('/ on floats', () => {
        const result = many(`create table test(a float, b float);
                            insert into test values (5, 2);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).toEqual([2.5]);
    });
    it('/ on float and int', () => {
        const result = many(`create table test(a float, b int);
                            insert into test values (5, 2);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).toEqual([2.5]);
    });

    it('* on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (4, 2);
                            select a*b as res from test`);
        expect(result.map(x => x.res)).toEqual([8]);
    });


    it('respects operator precedence', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 2);
                            select a + b * a as res from test`);
        expect(result.map(x => x.res)).toEqual([6]);
    });


    it('respects parenthesis', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 2);
                            select (a + b) * a as res from test`);
        expect(result.map(x => x.res)).toEqual([8]);
    });



    describe('IN operators', () => {

        it('"IN" clause with constants and no index', () => {
            simpleDb();
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
            const got = many(`select * from data where str in ('str1', 'str3')`);
            expect(trimNullish(got)).toEqual([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
        });

        it('"IN" clause with constants index', () => {
            simpleDb();
            db.public.none('create index on data(str)');
            preventSeqScan(db);
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
            const got = many(`select * from data where str in ('str1', 'str3')`);
            expect(trimNullish(got)).toEqual([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
        });

        it('"IN" behaves nicely with null left without index', () => {
            const got = many(`create table test(val text);
                insert into test values ('a'), ('b'), (null);
                select * from test where val in ('a', null)`);
            expect(trimNullish(got)).toEqual([{ val: 'a' }]);
        });

        it('"IN" behaves nicely with null left with index', () => {
            preventSeqScan(db);
            const got = many(`create table test(val text);
                create index on test(val);
                insert into test values ('a'), ('b'), (null);
                select * from test where val in ('a', null)`);
            expect(trimNullish(got)).toEqual([{ val: 'a' }]);
        });

        it('"IN" behaves nicely with null constant left without index', () => {
            const got = many(`create table test(val text);
                insert into test values ('a'), ('b'), (null);
                select * from test where null in (val, null)`);
            expect(trimNullish(got)).toEqual([]);
        });



        it('"IN" clause with no constant', () => {
            simpleDb();
            none(`insert into data(id, str, otherstr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')`);
            const got = many(`select * from data where id in (str, otherstr)`);
            expect(got.map(x => x.id)).toEqual(['A', 'C']);
        });

        it('"IN" clause with constant value', () => {
            simpleDb();
            none("insert into data(id, str, otherstr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')");
            const got = many(`select * from data where 'A' in (str, otherstr)`);
            expect(got.map(x => x.id)).toEqual(['A', 'C']);
        });

        it('"NOT IN" clause with constants and no index', () => {
            simpleDb();
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
            const got = many(`select * from data where str not in ('str1', 'str3')`);
            expect(trimNullish(got)).toEqual([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
        });

        it('"NOT IN" clause with constants index', () => {
            simpleDb();
            db.public.none('create index on data(str)');
            preventSeqScan(db);
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
            const got = many(`select * from data where str not in ('str1', 'str3')`);
            expect(trimNullish(got)).toEqual([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
        });



        // see #castArrayIn
        it('[bugfix] should convert str to int "in"', () => {
            expect(many(`
            create table test(vals int);
            insert into test values (1),(2),(3);
            select * from test where vals in ('1');
        `))
                .toEqual([{ vals: 1 }]);
        })

        it('[bugfix] should convert int to str on "not(in)"', () => {
            expect(many(`
            create table test(vals int);
            insert into test values (1),(2),(3);
            select * from test where NOT(vals in ('1'));
        `))
                .toEqual([{ vals: 2 }, { vals: 3 }]);
        })


        it('[bugfix] should convert int to str on "notin"', () => {
            expect(many(`
            create table test(vals int);
            insert into test values (1),(2),(3);
            select * from test where vals not in ('1');
        `))
                .toEqual([{ vals: 2 }, { vals: 3 }]);
        })

        it('[bugfix] fix in with literal int as string', () => {
            expect(many(`select 1 in ('1') as x`))
                .toEqual([{ x: true }]);
        })

    })



    describe('@> operator', () => {

        it('on value query', () => {
            const result = many(`create table test(id text primary key, data jsonb);
                                insert into test values ('id1', '{"prop": "A","in":1}'), ('id2', '{"prop": "B","in":2}'), ('id4', '{"prop": "A","in":3}'), ('id5', null);
                                select id from test where data @> '{"prop": "A"}';`);
            expect(result.map(x => x.id)).toEqual(['id1', 'id4']);
        });

        it('on array inverted', () => {
            expect(many(`create table test(id text, vals jsonb);
                            insert into test values ('a', '{"x":["a", "b", "c"]}'), ('b', '{"x":["a", "d"]}');
                            select id from test where vals @> '{"x": ["b", "a"]}';`))
                .toEqual([{ id: 'a' }]);
        });

        it('on array normal', () => {
            expect(many(`create table test(id text, vals jsonb);
                            insert into test values ('a', '{"x":["a", "b", "c"]}'), ('b', '{"x":["a", "d"]}');
                            select id from test where vals @> '{"x": ["a", "b"]}';`))
                .toEqual([{ id: 'a' }]);
        });


        it('on array single', () => {
            expect(many(`create table test(id text, vals jsonb);
                            insert into test values ('a', '{"x":["a", "b", "c"]}'), ('b', '{"x":["a", "d"]}');
                            select id from test where vals @> '{"x": ["a"]}';`))
                .toEqual([{ id: 'a' }, { id: 'b' }]);
        })
    });

    describe('&& operator', () => {

        function fill() {
            none(`create table test(id text primary key, data text array);
                insert into test values ('id1', '{"a", "b", "c"}'), ('id2', '{"b", "c", "d"}'), ('id4', '{"c", "d", "e"}'), ('id5', null);`)
        }

        it('finds overlap', () => {
            fill();
            const result = many(`select id from test where data && '{"b"}';`);
            expect(result.map((x) => x.id)).toEqual(['id1', 'id2']);
        });


        it('checks that any overlap is okay', () => {
            fill();
            const result = many(`select id from test where data && '{"b", "e"}';`);
            expect(result.map((x) => x.id)).toEqual(['id1', 'id2', 'id4']);
        });

        it('checks types', () => {
            fill();
            expectQueryError(() => {
                none(`select id from test where id && '{"b", "e"}'::text array;`)
            }, /Operator does not exist: text && text\[\]/);
        })

    });

    describe('LIKE operators', () => {

        it('executes like', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), (null);
                select * from test where val like 'fo%'`))
                .toEqual([
                    { val: 'foo' }
                    , { val: 'foobar' }
                ]);
        });

        it('executes like with _ token', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), (null);
                select * from test where val like 'fo_'`))
                .toEqual([
                    { val: 'foo' }
                ]);
        });

        it('executes ilike', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val ilike 'fo%'`))
                .toEqual([
                    { val: 'foo' }
                    , { val: 'foobar' }
                    , { val: 'FOOBAR' }
                ]);
        });


        it('executes pure "startsWith" like with index', () => {
            preventSeqScan(db);
            expect(many(`create table test(val text);
                create index on test(val);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val like 'fo%'`))
                .toEqual([
                    { val: 'foo' }
                    , { val: 'foobar' }
                ]);
        });

        for (const kind of ['asc', 'desc']) {
            it(`executes "startsWith" like with ${kind} index`, () => {
                preventSeqScan(db);
                expect(many(`create table test(val text);
                    create index on test(val ${kind});
                    insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                    select * from test where val like 'fo%b%'`))
                    .toEqual([
                        { val: 'foobar' }
                    ]);
            });
        }
        it('executes startsWith() like with index and _ token', () => {
            preventSeqScan(db);
            expect(many(`create table test(val text);
                create index on test(val);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val like 'fo_'`))
                .toEqual([
                    { val: 'foo' }
                ]);
        });

        it('executes like with index without token', () => {
            preventSeqScan(db);
            expect(many(`create table test(val text);
                create index on test(val);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val like 'foo'`))
                .toEqual([
                    { val: 'foo' }
                ]);
        });


        it('executes not like', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val not like 'fo%'`))
                .toEqual([
                    { val: 'bar' }
                    , { val: 'FOOBAR' }
                ]);
        });

        it('executes not ilike', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val not ilike 'fo%'`))
                .toEqual([
                    { val: 'bar' }
                ]);
        });
    })





    it('executes array index', () => {
        expect(many(`create table test(val integer[]);
                    insert into test values ('{1, 2, 3}');
                    select val[2] as x from test;`))
            .toEqual([{ x: 2 }]) // <== 1-based !
    });

    it('executes array multiple index', () => {
        expect(many(`create table test(val integer[][]);
                insert into test values ('{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}');
                select val[2][2] as x from test;`))
            .toEqual([{ x: 5 }])
    });

    it('executes array multiple index incomplete indexing', () => {
        expect(many(`create table test(val integer[][]);
                insert into test values ('{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}');
                select val[2] as incomplete, val[2][1] as complete from test;`))
            .toEqual([{ incomplete: null, complete: 4 }])
    });

    describe('Between operator', () => {

        for (const x of [
            { query: `select 42 between 1 and 100 as x`, result: { x: true } }
            , { query: `select 101 between 1 and 100 as x`, result: { x: false } }
            , { query: `select 0 between 1 and 100 as x`, result: { x: false } }
            , { query: `select 1 between 1 and 100 as x`, result: { x: true } }
            , { query: `select 100 between 1 and 100 as x`, result: { x: true } }
            , { query: `select '99' between '1' and 100 as x`, result: { x: true } }
            , { query: `select 42 between null and 2 as x`, result: { x: false } }
            , { query: `select 2 between null and 42 as x`, result: { x: null } }
            , { query: `select 42 between 5 and null as x`, result: { x: null } }
            , { query: `select 42 between 100 and null as x`, result: { x: false } }]) {
            it('can select between: ' + x.query, () => {
                expect(many(x.query))
                    .toEqual([x.result])
            });
        }


        for (const x of [
            { query: `select 42 not between 1 and 100 as x`, result: { x: false } }
            , { query: `select 101 not between 1 and 100 as x`, result: { x: true } }
            , { query: `select 0 not between 1 and 100 as x`, result: { x: true } }
            , { query: `select 1 not between 1 and 100 as x`, result: { x: false } }
            , { query: `select 100 not between 1 and 100 as x`, result: { x: false } }
            , { query: `select '99' not between '1' and 100 as x`, result: { x: false } }
            , { query: `select 42 not between null and 2 as x`, result: { x: true } }
            , { query: `select 2 not between null and 42 as x`, result: { x: null } }
            , { query: `select 42 not between 5 and null as x`, result: { x: null } }
            , { query: `select 42 not between 100 and null as x`, result: { x: true } }]) {
            it('can select not between: ' + x.query, () => {
                expect(many(x.query))
                    .toEqual([x.result])
            });
        }

        it('cannot select those betweens', () => {
            expectQueryError(() => many(`select 'yo' between '1' and 100 as x`));
            expectQueryError(() => many(`select 10 between '1' and 'yo' as x`));
        });


        it('uses index while using between', () => {
            preventSeqScan(db);
            const got = many(`create table test(num integer primary key);
                            insert into test values (0), (1), (50), (100), (101);
                            select * from test where num between 1 and 100;`);
            expect(got)
                .toEqual([{ num: 1 }
                    , { num: 50 }
                    , { num: 100 }]);
            expect(db.public.explainLastSelect()).toEqual({
                _: 'inside',
                id: 1,
                entropy: 3,
                on: {
                    _: 'btree',
                    btree: ['num'],
                    onTable: 'test',
                }
            });
        });
        it('uses index while using not between', () => {
            preventSeqScan(db);
            expect(many(`create table test(num integer primary key);
                            insert into test values (0), (1), (50), (100), (101);
                            select * from test where num not between 1 and 100;`))
                .toEqual([{ num: 0 }
                    , { num: 101 }])

            expect(db.public.explainLastSelect()).toEqual({
                _: 'outside',
                id: 1,
                entropy: 2,
                on: {
                    _: 'btree',
                    btree: ['num'],
                    onTable: 'test',
                }
            })
        });
    })
});
