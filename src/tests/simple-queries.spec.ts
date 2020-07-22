import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('Simple queries', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.query.many.bind(db.query);
        none = db.query.none.bind(db.query);
    });

    function simpleDb() {
        db.declareTable({
            name: 'data',
            fields: [{
                id: 'id',
                type: Types.text(),
                primary: true,
            }, {
                id: 'str',
                type: Types.text(),
            }, {
                id: 'otherStr',
                type: Types.text(),
            }],
        });
        return db;
    }

    it('where on primary', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id) values ('some value')`);
        let got = many(`select * from data where id='SOME ID'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where id='some value'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);

    });


    it('where constant true', () => {
        simpleDb();
        none(`insert into data(id) values ('some value')`);
        let got = many('select * from data where 1 = 1');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);
    });

    it('where constant false', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id) values ('some value')`);
        let got = many('select * from data where 1 = 0');
        expect(trimNullish(got)).to.deep.equal([]);
    });

    it('where on other', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where str='none'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where str='some str'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('can insert and select null', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', null)`);
        let got = many('select * from data where str is null');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id' }]);
        got = many('select * from data where str is not null');
        expect(got).to.deep.equal([]);
    });

    it('does not equate null values on seq scan', () => {
        simpleDb();
        none(`insert into data(id, str, otherStr) values ('id1', null, null)`);
        none(`insert into data(id, str, otherStr) values ('id2', 'A', 'A')`);
        none(`insert into data(id, str, otherStr) values ('id3', 'A', 'B')`);
        none(`insert into data(id, str, otherStr) values ('id4', null, 'B')`);
        none(`insert into data(id, str, otherStr) values ('id5', 'A', null)`);
        const got = many('select * from data where str = otherStr');
        expect(got).to.deep.equal([{ id: 'id2', str: 'A', otherStr: 'A' }]);
    });

    function setupNulls() {
        const db = simpleDb()
        db.getTable('data')
            .createIndex(['str']);
        none(`insert into data(id, str) values ('id1', null)`);
        none(`insert into data(id, str) values ('id2', 'notnull2')`);
        none(`insert into data(id, str) values ('id3', null)`);
        none(`insert into data(id, str) values ('id4', 'notnull4')`);
        return db;
    }

    it('uses indexes for null values', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = many('select * from data where str is null');
        expect(got).to.deep.equal([{ id: 'id1', str: null }, { id: 'id3', str: null }]);
    });


    it('uses indexes for not null values', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = many('select * from data where str is not null');
        expect(got).to.deep.equal([{ id: 'id2', str: 'notnull2' }, { id: 'id4', str: 'notnull4' }]);
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
            db.getTable('data').createIndex(['str']);
            preventSeqScan(db);
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
            const got = many(`select * from data where str in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
        });

        it('"IN" clause with no constant', () => {
            simpleDb();
            none(`insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')`);
            const got = many(`select * from data where id in (str, otherStr)`);
            expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
        });

        it('"IN" clause with constant value', () => {
            simpleDb();
            none("insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')");
            const got = many(`select * from data where 'A' in (str, otherStr)`);
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
            db.getTable('data').createIndex(['str']);
            preventSeqScan(db);
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
            const got = many(`select * from data where str not in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
        });

    })

    it('AND query', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where id='some id' AND str='other'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where id='some id' and str='some str'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('OR query', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where id='other' OR str='other'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where id='some id' OR str='other'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
        got = many(`select * from data where id='some id' or str='some str'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('@> on value query', () => {
        const result = many(`create table test(id text primary key, data jsonb);
                            insert into test values ('id1', '{"prop": "A","in":1}'), ('id2', '{"prop": "B","in":2}'), ('id4', '{"prop": "A","in":3}'), ('id5', null);
                            select id from test where data @> '{"prop": "A"}';`);
        expect(result.map(x => x.id)).to.deep.equal(['id1', 'id4']);
    });


    it('call lower in select', () => {
        simpleDb();
        none(`insert into data(id) values ('SOME STRING')`);
        const result = many(`select lower(id) from data`);
        expect(result).to.deep.equal([{ column0: 'some string' }]);
    });

    it('call lower in condition', () => {
        simpleDb();
        none(`insert into data(id, str) values ('id1', 'SOME STRING'), ('id2', 'other string'), ('id3', 'Some String')`);
        const result = many(`select id from data where lower(str)='some string'`);
        expect(result.map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });


    it('can select current_schema', () => {
        simpleDb();
        expect(many('select * from current_schema')).to.deep.equal([{ current_schema: 'public' }]);
    });


    it('can select info tables', () => {
        simpleDb();
        expect(many('select table_name from information_schema.tables')).to.deep.equal([{ table_name: 'data' }]);
    });


    it('can select info columns', () => {
        simpleDb();
        expect(many(`select column_name from information_schema.columns where table_name='data'`))
            .to.deep.equal([{ column_name: 'id' }
                , { column_name: 'str' }
                , { column_name: 'otherStr' }]);
    });


    it('selects case whithout condition', () => {
        simpleDb();
        expect(many(`insert into data(id, str) values ('id1', 'SOME STRING'), ('id2', 'other string'), ('id3', 'Some String');
            select case when id='id1' then 'one ' || str else 'something else' end as x from data`))
            .to.deep.equal([{ x: 'one SOME STRING' }, { x: 'something else' }, { x: 'something else' }]);
    })

    it('selects case with disparate types results', () => {
        simpleDb();
        expect(many(`select case when 2 > 1 then 1.5 when 2 < 1 then 1 end as x`))
            .to.deep.equal([{ x: 1.5 }]);
    })


    describe('Indexes on comparisons', () => {

        it('uses asc index on > comparison', () => {
            preventSeqScan(db);
            const result = many(`create table test(val integer);
                                create index on test(val);
                                insert into test values (1), (2), (3), (4);
                                select * from test where val > 2`);
            expect(result).to.deep.equal([{ val: 3 }, { val: 4 }]);
        });

        it('uses desc index on > comparison', () => {
            preventSeqScan(db);
            const result = many(`create table test(val integer);
                                create index on test(val desc);
                                insert into test values (1), (2), (3), (4);
                                select * from test where val > 2`);
            expect(result).to.deep.equal([{ val: 3 }, { val: 4 }]);
        });


        it('uses asc index on < comparison', () => {
            preventSeqScan(db);
            const result = many(`create table test(val integer);
                                create index on test(val);
                                insert into test values (1), (2), (3), (4);
                                select * from test where val < 3`);
            expect(result).to.deep.equal([{ val: 1 }, { val: 2 }]);
        });

        it('uses desc index on < comparison', () => {
            preventSeqScan(db);
            const result = many(`create table test(val integer);
                                create index on test(val desc);
                                insert into test values (1), (2), (3), (4);
                                select * from test where val < 3`);
            expect(result).to.deep.equal([{ val: 1 }, { val: 2 }]);
        });

        it('uses index on <= comparison', () => {
            preventSeqScan(db);
            const result = many(`create table test(val integer);
                                create index on test(val);
                                insert into test values (1), (2), (3), (4);
                                select * from test where val <= 2`);
            expect(result).to.deep.equal([{ val: 1 }, { val: 2 }]);
        });


        it('uses index on >= comparison', () => {
            preventSeqScan(db);
            const result = many(`create table test(val integer);
                                create index on test(val);
                                insert into test values (1), (2), (3), (4);
                                select * from test where val >= 2`);
            expect(result).to.deep.equal([{ val: 2 }, { val: 3 }, { val: 4 }]);
        });

    })


    it ('checks this is an invalid syntax', () => {
        assert.throws(() => none(`create table test(val integer);
                create index on test(val);
                insert into test values (1), (2), (3), (4)
                select * from test where val >= 2;`)); //   ^  missing a ";" ... but was not throwing.
    })

    describe('Implicit casts', () => {
        it('implicitely casts in case', () => {
            expect(many(`select  case when 2 > 1 then to_date('20170103','YYYYMMDD') else '2017-01-03' end as x;`))
                .to.deep.equal([{ x: new Date('2017-01-03') }]);
            expect(many(`select  case when 2 > 1 then to_date('20170103','YYYYMMDD') when 2 > 3 then '2017-01-03' end as x;`))
                .to.deep.equal([{ x: new Date('2017-01-03') }]);
            expect(many(`select  case when 2 > 1 then '2017-01-03' else to_date('20170103','YYYYMMDD') end as x;`))
                .to.deep.equal([{ x: new Date('2017-01-03') }]);
        });

        it('implicitely casts in +', () => {
            expect(many(`select  1.5 + 1 as x;`))
                .to.deep.equal([{ x: 2.5 }]);
            expect(many(`select  1 + 1.5 as x;`))
                .to.deep.equal([{ x: 2.5 }]);
        });

        it('implicitely casts in + from int table', () => {
            none('create table test(num int); insert into test values (1)')
            expect(many(`select  1.5 + num as x from test`))
                .to.deep.equal([{ x: 2.5 }]);
            expect(many(`select  num + 1.5 as x from test`))
                .to.deep.equal([{ x: 2.5 }]);
        });

        it('implicitely casts in + from float table', () => {
            none('create table test(num float); insert into test values (1.5)')
            expect(many(`select  1 + num as x from test`))
                .to.deep.equal([{ x: 2.5 }]);
            expect(many(`select  num + 1 as x from test`))
                .to.deep.equal([{ x: 2.5 }]);
        });

        it('implicitely casts int & string', () => {
            expect(many(`select 1 = '1' as x;`))
                .to.deep.equal([{ x: true }]);
        })

        it('implicitely casts float & string', () => {
            expect(many(`select 1.1 = '1.10' as x;`))
                .to.deep.equal([{ x: true }]);
        });

        it('does not implicitely cast float & string int', () => {
            assert.throws(() => many(`select 1 = '1.10' as x;`));
        });


        it('does not implicitely casts on operations even constant on case', () => {
            assert.throw(() => many(`select  case when 2 > 1 then to_date('20170103','YYYYMMDD') else ('2017-' || '01-03') end as x;`));
        });

        it('does not implicitely casts on operations even constant on comparison', () => {
            expect(many(`select to_date('20170103','YYYYMMDD') > '2017-01-03' as x;`))
                .to.deep.equal([{ x: false }]);
            assert.throw(() => many(`select to_date('20170103','YYYYMMDD') > ('2017-' || '01-03') as x;`));
        })

    });


    it('does not support select * on dual', () => {
        assert.throw(() => many(`select *`));
    });

    it('supports concat operator', () => {
        expect(many(`select 'a' || 'b' as x`))
            .to.deep.equal([{ x: 'ab' }]);
    });


    it('supports to_date function', () => {
        expect(many(`select to_date('20170103','YYYYMMDD') as x`))
            .to.deep.equal([{ x: new Date('2017-01-03') }]);
        expect(many(`select to_date('20170103',null) as x`))
            .to.deep.equal([{ x: null }]);
        expect(many(`select to_date(NULL, 'YYYYMMDD') as x`))
            .to.deep.equal([{ x: null }]);
        assert.throws(() => many(`select to_date('invalid date','YYYYMMDD') as x`));
    });


    it('executes member get text ->>', () => {
        none(`create table test(val jsonb);
            insert into test values ('{"prop": "str"}'), ('{"prop": 42}'), ('{"prop": [42, "val"]}')`);
        expect(many(`select val->>'prop' as x from test`))
            .to.deep.equal([
                { x: 'str' }
                , { x: '42' }
                , { x: `[42,"val"]` }
            ])
    });


    it('executes member get text ->', () => {
        none(`create table test(val jsonb);
            insert into test values ('{"prop": "str"}'), ('{"prop": 42}'), ('{"prop": [42]}')`);
        expect(many(`select val->'prop' as x from test`))
            .to.deep.equal([
                { x: 'str' }
                , { x: 42 }
                , { x: [42] }
            ])
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
                    , { val: null }
                ]);
        });

        it('executes not ilike', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val not ilike 'fo%'`))
                .to.deep.equal([
                    { val: 'bar' }
                    , { val: null }
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
                select val[2] as x from test;`))
            .to.deep.equal([{ x: null }])
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
            expect(many(`create table test(num integer primary key);
                            insert into test values (0), (1), (50), (100), (101);
                            select * from test where num between 1 and 100;`))
                .to.deep.equal([{ num: 1 }
                    , { num: 50 }
                    , { num: 100 }])
        });
        it('uses index while using not between', () => {
            preventSeqScan(db);
            expect(many(`create table test(num integer primary key);
                            insert into test values (0), (1), (50), (100), (101);
                            select * from test where num not between 1 and 100;`))
                .to.deep.equal([{ num: 0 }
                    , { num: 101 }])
        });
    })

    for (const x of [
        { query: `select '2' = any('{1,2}') x;`, result: { x: true } }
        , { query: `select 2.0 = any('{1,2}') x;`, result: { x: true } } // <== with implicit cast
        , { query: `select 2.1 = any('{1,2}') x;`, result: { x: true } }
        , { query: `select 'foo' like any('{%OO%}') x;`, result: { x: true } }
        , { query: `select 'bar' like any('{%OO%}') x;`, result: { x: false } }
    ]) {
        it('can execute ANY(): ' + x.query, () => {
            expect(many(x.query))
                .to.deep.equal([x.result]);
        })
    }
});
