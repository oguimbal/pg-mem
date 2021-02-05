import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { expectSingle, preventSeqScan } from './test-utils';
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
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
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


    it('can insert and select null', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', null)`);
        let got = many('select * from data where str is null');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id' }]);
        got = many('select * from data where str is not null');
        expect(got).to.deep.equal([]);
    });



    it('can select NOT', () => {
        expect(many(`select not true as v`))
            .to.deep.equal([{ v: false }])
    });

    it('can select negate', () => {
        expect(many(`select -(42) as v`))
            .to.deep.equal([{ v: -42 }])
    });

    it('does not return twice the same entity on seq scan', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', null)`);
        const [first] = many('select * from data');
        const [second] = many('select * from data');
        expect(first).to.deep.equal(second);
        expect(first).not.to.equal(second);//<== should be a copy
    });

    it('does not return twice the same entity on index scan', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', null)`);
        const [first] = many(`select * from data where id='some id'`);
        const [second] = many(`select * from data where id='some id'`);
        expect(first).to.deep.equal(second);
        expect(first).not.to.equal(second);//<== should be a copy
    });

    it('does not equate null values on seq scan', () => {
        simpleDb();
        none(`insert into data(id, str, otherstr) values ('id1', null, null)`);
        none(`insert into data(id, str, otherstr) values ('id2', 'A', 'A')`);
        none(`insert into data(id, str, otherstr) values ('id3', 'A', 'B')`);
        none(`insert into data(id, str, otherstr) values ('id4', null, 'B')`);
        none(`insert into data(id, str, otherstr) values ('id5', 'A', null)`);
        const got = many('select * from data where str = otherstr');
        expect(got).to.deep.equal([{ id: 'id2', str: 'A', otherstr: 'A' }]);
    });


    it('AND query', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where id='some id' AND str='other'`);
        expect(trimNullish(got)).to.deep.equal([]);
        got = many(`select * from data where id='some id' and str='some str'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('OR query', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where id='other' OR str='other'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where id='some id' OR str='other'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id', str: 'some str' }]);
        got = many(`select * from data where id='some id' or str='some str'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });



    it('insert returning', () => {
        expect(many(`create table test(id serial primary key, val text, nl text);
                                insert into test(val) values ('a'), ('b') returning id, val;`))
            .to.deep.equal([{ id: 1, val: 'a' }, { id: 2, val: 'b' }]);
    });


    it('call lower in select', () => {
        simpleDb();
        none(`insert into data(id) values ('SOME STRING')`);
        const result = many(`select lower(id) from data`);
        expect(result).to.deep.equal([{ lower: 'some string' }]);
    });

    it('cannot select from table with function syntax', () => {
        simpleDb();
        assert.throws(() => many(`select * from data()`));
    });

    it('aliases are case insensitive', () => {
        simpleDb();
        none(`select xx.ID from data as XX`);
    });

    it('now() does not behave as dual table', () => {
        assert.throws(() => many(`select * from now`), /relation "now" does not exist/);
    })

    it('can select * from now()', () => {
        const ret = many(`select * from now()`);
        expect(ret.length).to.equal(1);
        expect(ret[0].now).to.be.instanceOf(Date);
    });

    it('can select now()', () => {
        expect(many(`select now()`)[0])
            .to.have.property('now');
    })

    it('can select current_schema as classic table', () => {
        simpleDb();
        expect(many('select * from current_schema')).to.deep.equal([{ current_schema: 'public' }]);
    });


    it('can select current_schema as table function', () => {
        simpleDb();
        expect(many('select * from current_schema()')).to.deep.equal([{ current_schema: 'public' }]);
    });


    it('can select current_schema as function', () => {
        simpleDb();
        expect(many('select current_schema()')).to.deep.equal([{ current_schema: 'public' }]);
    });

    it('can select current_schema as const', () => {
        simpleDb();
        expect(many('select current_schema')).to.deep.equal([{ current_schema: 'public' }]);
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
                , { column_name: 'otherstr' }]);
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



    it('auto increments values', () => {
        expect(many(`create table test(id serial, txt text);
                    insert into test(txt) values ('a'), ('b');
                    select * from test;`))
            .to.deep.equal([{ id: 1, txt: 'a' }, { id: 2, txt: 'b' }])
    });

    it('not null does not accept null values', () => {
        assert.throws(() => none(`create table test(txt text not null);
                    insert into test(txt) values (null);`));
    });


    it('can create columns not null with default', () => {
        expect(many(`create table test(id text, val text not null default 'def');
                    insert into test(id) values ('id');
                    select * from test`))
            .to.deep.equal([{ id: 'id', val: 'def' }]);
    });


    it('can create columns moving constant defaults', async () => {
        const orig = many(`create table test(id text, time timestamp default now());
                    insert into test(id) values ('id1') returning time;`)
            .map(x => x.time)[0];
        assert.instanceOf(orig, Date);
        await new Promise(done => setTimeout(done, 5)); // wait 5 ms
        const newtime = many(`insert into test(id) values ('id2') returning time;`)
            .map(x => x.time)[0];
        assert.instanceOf(newtime, Date);
        expect(orig).not.to.equal(newtime);
    });




    describe('ANY() and operators', () => {
        for (const x of [
            { query: `select '2' = any('{1,2}') x;`, result: { x: true } }
            , { query: `select 2.0 = any('{1,2}') x;`, result: { x: true } } // <== with implicit cast
            , { query: `select 2.1 = any('{1,2}') x;`, result: { x: false } }
            , { query: `select 'foo' like any('{%OO%}') x;`, result: { x: false } }
            , { query: `select 'foo' like any('{%oo%}') x;`, result: { x: true } }
            , { query: `select 'bar' like any('{%OO%}') x;`, result: { x: false } }
        ]) {
            it('can execute ANY(): ' + x.query, () => {
                expect(many(x.query))
                    .to.deep.equal([x.result]);
            })
        }

        it('can execute any on a selection', () => {
            expect(many(`create table vals(val int);
                            insert into vals values (0), (1), (50), (100), (101);
                            select 50 = any(select * from vals) as x`))
                .to.deep.eq([{ x: true }])
            expect(many(`select 42 = any(select * from vals) as x`))
                .to.deep.eq([{ x: false }])

        })
    })

    it('can create table with compound primary key', () => {
        none(`create table test(ka text, kb integer, val text,  primary key (ka, kb));
            insert into test values ('a', 1, 'oldA');`);
        assert.throws(() => none(`insert into test values ('a', 1, 'oldA');`));
    });

    it('supports substring() query as range', () => {
        expect(many(`select substring('012345678' from 2 for 3) as v`))
            .to.deep.equal([{ v: '123' }]);
        expect(many(`select substring('012345678' from 5) as v`))
            .to.deep.equal([{ v: '45678' }]);
        expect(many(`select substring('012345678' for 3) as v`))
            .to.deep.equal([{ v: '012' }]);
    })



    it('can select list', () => {
        expect(many(`select (1, 2) as v`))
            .to.deep.equal([{ v: [1, 2] }]);
    })


    it('can convert list to text', () => {
        expect(many(`select (1, 2)::text as v`))
            .to.deep.equal([{ v: '(1,2)' }]);
    })

    it('supports select ARRAY[]', () => {
        expect(many(`select ARRAY[1, 2] as v`))
            .to.deep.equal([{ v: [1, 2] }])
    });

    it('can convert array to text', () => {
        expect(many(`select ARRAY[1, 2]::text as v`))
            .to.deep.equal([{ v: '{1,2}' }]);
    })

    it('can execute ANY on ARRAY[]', () => {
        expect(many(`create table tbl (id text);
                    insert into tbl values ('A'), ('B'), ('C');
                    SELECT * FROM tbl WHERE id = ANY(array['A', 'C'])`))
            .to.deep.equal([{ id: 'A' }, { id: 'C' }]);
    })
});
