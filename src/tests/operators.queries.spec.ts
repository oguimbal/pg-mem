import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
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
        expect(result.map(x => x.res)).to.deep.equal([3]);
    });

    it('date + interval', () => {
        const result = many(`select  interval '1 day' + now()::date as dt`);
        const dt = result[0]?.dt
        assert.instanceOf(dt, Date);
        expect(dt.toString()).to.equal(moment.utc().startOf('day').add(1, 'day').toDate().toString());
    });


    it('timestamp + interval', () => {
        const result = many(`select now() + interval '1 day' as dt`);
        const dt = result[0]?.dt
        assert.instanceOf(dt, Date);
        expect(moment(dt).startOf('second').toISOString()).to.equal(moment.utc().startOf('second').add(1, 'day').toISOString());
    });


    it('interval + timestamp', () => {
        const result = many(`select interval '1 day' + now() as dt`);
        const dt = result[0]?.dt
        assert.instanceOf(dt, Date);
        expect(moment(dt).startOf('second').toISOString()).to.equal(moment.utc().startOf('second').add(1, 'day').toISOString());
    });

    it('timestamp - interval', () => {
        const result = many(`select now() - interval '1 day' as dt`);
        const dt = result[0]?.dt
        assert.instanceOf(dt, Date);
        expect(moment(dt).startOf('second').toISOString()).to.equal(moment.utc().startOf('second').add(-1, 'day').toISOString());
    });

    it('- on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 1);
                            select a-b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([1]);
    });

    it('/ on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (17, 10);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([1]); // trunc is used on divisions
    });

    it('/ on neg ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (-17, 10);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([-1]); // trunc is used on divisions
    });

    it('/ on int literals', () => {
        expect(one(`select 17/10 as res`)).to.deep.equal({ res: 1 });
    })

    it('/ on float literals on second arg', () => {
        expect(one(`select 17/10.0 as res`)).to.deep.equal({ res: 1.7 });
    });

    it('/ on float literals on first arg arg', () => {
        expect(one(`select 18.0/10 as res`)).to.deep.equal({ res: 1.8 });
    })

    it('/ on floats', () => {
        const result = many(`create table test(a float, b float);
                            insert into test values (5, 2);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([2.5]);
    });
    it('/ on float and int', () => {
        const result = many(`create table test(a float, b int);
                            insert into test values (5, 2);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([2.5]);
    });

    it('* on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (4, 2);
                            select a*b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([8]);
    });


    it('respects operator precedence', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 2);
                            select a + b * a as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([6]);
    });


    it('respects parenthesis', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 2);
                            select (a + b) * a as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([8]);
    });



    describe('IN operators', () => {

        it('"IN" clause with constants and no index', () => {
            simpleDb();
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
            const got = many(`select * from data where str in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
        });

        it('"IN" clause with constants index', () => {
            simpleDb();
            db.public.none('create index on data(str)');
            preventSeqScan(db);
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
            const got = many(`select * from data where str in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
        });

        it('"IN" behaves nicely with null left without index', () => {
            const got = many(`create table test(val text);
                insert into test values ('a'), ('b'), (null);
                select * from test where val in ('a', null)`);
            expect(trimNullish(got)).to.deep.equal([{ val: 'a' }]);
        });

        it('"IN" behaves nicely with null left with index', () => {
            preventSeqScan(db);
            const got = many(`create table test(val text);
                create index on test(val);
                insert into test values ('a'), ('b'), (null);
                select * from test where val in ('a', null)`);
            expect(trimNullish(got)).to.deep.equal([{ val: 'a' }]);
        });

        it('"IN" behaves nicely with null constant left without index', () => {
            const got = many(`create table test(val text);
                insert into test values ('a'), ('b'), (null);
                select * from test where null in (val, null)`);
            expect(trimNullish(got)).to.deep.equal([]);
        });



        it('"IN" clause with no constant', () => {
            simpleDb();
            none(`insert into data(id, str, otherstr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')`);
            const got = many(`select * from data where id in (str, otherstr)`);
            expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
        });

        it('"IN" clause with constant value', () => {
            simpleDb();
            none("insert into data(id, str, otherstr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')");
            const got = many(`select * from data where 'A' in (str, otherstr)`);
            expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
        });

        it('"NOT IN" clause with constants and no index', () => {
            simpleDb();
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
            const got = many(`select * from data where str not in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
        });

        it('"NOT IN" clause with constants index', () => {
            simpleDb();
            db.public.none('create index on data(str)');
            preventSeqScan(db);
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
            const got = many(`select * from data where str not in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
        });



        // see #castArrayIn
        it('[bugfix] should convert str to int "in"', () => {
            expect(many(`
            create table test(vals int);
            insert into test values (1),(2),(3);
            select * from test where vals in ('1');
        `))
                .to.deep.equal([{ vals: 1 }]);
        })

        it('[bugfix] should convert int to str on "not(in)"', () => {
            expect(many(`
            create table test(vals int);
            insert into test values (1),(2),(3);
            select * from test where NOT(vals in ('1'));
        `))
                .to.deep.equal([{ vals: 2 }, { vals: 3 }]);
        })


        it('[bugfix] should convert int to str on "notin"', () => {
            expect(many(`
            create table test(vals int);
            insert into test values (1),(2),(3);
            select * from test where vals not in ('1');
        `))
                .to.deep.equal([{ vals: 2 }, { vals: 3 }]);
        })

        it('[bugfix] fix in with literal int as string', () => {
            expect(many(`select 1 in ('1') as x`))
                .to.deep.equal([{ x: true }]);
        })

    })



    describe('@> operator', () => {

        it('on value query', () => {
            const result = many(`create table test(id text primary key, data jsonb);
                                insert into test values ('id1', '{"prop": "A","in":1}'), ('id2', '{"prop": "B","in":2}'), ('id4', '{"prop": "A","in":3}'), ('id5', null);
                                select id from test where data @> '{"prop": "A"}';`);
            expect(result.map(x => x.id)).to.deep.equal(['id1', 'id4']);
        });

        it('on array inverted', () => {
            expect(many(`create table test(id text, vals jsonb);
                            insert into test values ('a', '{"x":["a", "b", "c"]}'), ('b', '{"x":["a", "d"]}');
                            select id from test where vals @> '{"x": ["b", "a"]}';`))
                .to.deep.equal([{ id: 'a' }]);
        });

        it('on array normal', () => {
            expect(many(`create table test(id text, vals jsonb);
                            insert into test values ('a', '{"x":["a", "b", "c"]}'), ('b', '{"x":["a", "d"]}');
                            select id from test where vals @> '{"x": ["a", "b"]}';`))
                .to.deep.equal([{ id: 'a' }]);
        });


        it('on array single', () => {
            expect(many(`create table test(id text, vals jsonb);
                            insert into test values ('a', '{"x":["a", "b", "c"]}'), ('b', '{"x":["a", "d"]}');
                            select id from test where vals @> '{"x": ["a"]}';`))
                .to.deep.equal([{ id: 'a' }, { id: 'b' }]);
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
            expect(result.map((x) => x.id)).to.deep.equal(['id1', 'id2']);
        });


        it('checks that any overlap is okay', () => {
            fill();
            const result = many(`select id from test where data && '{"b", "e"}';`);
            expect(result.map((x) => x.id)).to.deep.equal(['id1', 'id2', 'id4']);
        });

        it('checks types', () => {
            fill();
            assert.throws(() => {
                none(`select id from test where id && '{"b", "e"}'::text array;`)
            }, /Operator does not exist: text && text\[\]/);
        })

    });

    describe('LIKE operators', () => {

        it('executes like', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), (null);
                select * from test where val like 'fo%'`))
                .to.deep.equal([
                    { val: 'foo' }
                    , { val: 'foobar' }
                ]);
        });

        it('executes like with _ token', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), (null);
                select * from test where val like 'fo_'`))
                .to.deep.equal([
                    { val: 'foo' }
                ]);
        });

        it('executes ilike', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val ilike 'fo%'`))
                .to.deep.equal([
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
                .to.deep.equal([
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
                    .to.deep.equal([
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
                .to.deep.equal([
                    { val: 'foo' }
                ]);
        });

        it('executes like with index without token', () => {
            preventSeqScan(db);
            expect(many(`create table test(val text);
                create index on test(val);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val like 'foo'`))
                .to.deep.equal([
                    { val: 'foo' }
                ]);
        });


        it('executes not like', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val not like 'fo%'`))
                .to.deep.equal([
                    { val: 'bar' }
                    , { val: 'FOOBAR' }
                ]);
        });

        it('executes not ilike', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val not ilike 'fo%'`))
                .to.deep.equal([
                    { val: 'bar' }
                ]);
        });
    })





    it('executes array index', () => {
        expect(many(`create table test(val integer[]);
                    insert into test values ('{1, 2, 3}');
                    select val[2] as x from test;`))
            .to.deep.equal([{ x: 2 }]) // <== 1-based !
    });

    it('executes array multiple index', () => {
        expect(many(`create table test(val integer[][]);
                insert into test values ('{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}');
                select val[2][2] as x from test;`))
            .to.deep.equal([{ x: 5 }])
    });

    it('executes array multiple index incomplete indexing', () => {
        expect(many(`create table test(val integer[][]);
                insert into test values ('{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}');
                select val[2] as incomplete, val[2][1] as complete from test;`))
            .to.deep.equal([{ incomplete: null, complete: 4 }])
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
                    .to.deep.equal([x.result])
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
                    .to.deep.equal([x.result])
            });
        }

        it('cannot select those betweens', () => {
            assert.throws(() => many(`select 'yo' between '1' and 100 as x`));
            assert.throws(() => many(`select 10 between '1' and 'yo' as x`));
        });


        it('uses index while using between', () => {
            preventSeqScan(db);
            const got = many(`create table test(num integer primary key);
                            insert into test values (0), (1), (50), (100), (101);
                            select * from test where num between 1 and 100;`);
            expect(got)
                .to.deep.equal([{ num: 1 }
                    , { num: 50 }
                    , { num: 100 }]);
            assert.deepEqual(db.public.explainLastSelect(), {
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
                .to.deep.equal([{ num: 0 }
                    , { num: 101 }])

            assert.deepEqual(db.public.explainLastSelect(), {
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
