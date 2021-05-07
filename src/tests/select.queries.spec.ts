import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { Types } from '../datatypes';
import { _IDb } from '../interfaces-private';
import { SelectFromStatement, SelectStatement } from 'pgsql-ast-parser';
import { buildValue } from '../expression-builder';
import { parseSql } from '../parse-cache';

describe('Selections', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
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
            .to.deep.equal([{}]);
    });

    it('can use transformations', () => {
        stuff();
        expect(many(`select * from (select val as xx from test where txt = 'A') x where x.xx >= 1`))
            .to.deep.equal([{ xx: 999 }, { xx: 1 }]);
    });

    it('executes the right plan on transformations', () => {
        stuff();
        const plan = (db as _IDb).public.explainSelect(`select * from (select val as valAlias from test where txt = 'A') x where x.valAlias >= 1`);
        // assert.deepEqual(plan, {} as any);
        assert.deepEqual(plan, {
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
            .to.deep.equal([{ txtx: 'A', valx: 999, v: 'a' }, { txtx: 'A', valx: 1, v: 'a' }]);
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

    it('does not support select * on dual', () => {
        assert.throw(() => many(`select *`));
    });

    it('supports concat operator', () => {
        expect(many(`select 'a' || 'b' as x`))
            .to.deep.equal([{ x: 'ab' }]);
    });

    it('has an index', () => {
        simpleDb();
        const [{ where }] = parseSql(`select * from data where id='x'`) as SelectFromStatement[];
        if (!where || where.type !== 'binary') {
            assert.fail('Should be a binary');
        }
        const built = buildValue(db.getTable('data').selection, where.left);
        assert.exists(built.index);
    });


    it('detects ambiguous column selections on aliases', () => {
        // same-name columns not supported...if supported, must continue to throw when doing this:
        assert.throws(() => none(`create table data(id text primary key, str text);
            select x.a from (select id as a, str as a from data) x;`), /column reference "a" is ambiguous/);
    });





    it('can select from values', () => {
        expect(many(`select * from (values (1, 'one'), (2, 'two')) as  tbl (num, str);`))
            .to.deep.equal([
                { num: 1, str: 'one' },
                { num: 2, str: 'two' },
            ])
    })


    it('can select qualified from values', () => {
        expect(many(`select tbl.num, tbl.str from (values (1, 'one'), (2, 'two')) as  tbl (num, str);`))
            .to.deep.equal([
                { num: 1, str: 'one' },
                { num: 2, str: 'two' },
            ])
    })

    it('can filter from values', () => {
        expect(many(`select * from (values (1, 'one'), (2, 'two')) as  tbl (num, str)  where tbl.num>1;`))
            .to.deep.equal([
                { num: 2, str: 'two' },
            ])
    });

    it('can select from function with selection', () => {
        expect(many(`select a,b from concat('a') as a join concat('a') as b on b=a`))
            .to.deep.equal([{ a: 'a', b: 'a' }])
    })

    it('can select from function without alias', () => {
        expect(many(`select * from concat('a') as a join concat('a') as b on b=a`))
            .to.deep.equal([{ a: 'a', b: 'a' }])
    })

    it('can select from function with alias', () => {
        expect(many(`select * from concat('a') as a join concat('a') as b on a.a=b.b`))
            .to.deep.equal([{ a: 'a', b: 'a' }])
    })


    it('can select record when not aliased', () => {
        expect(many(`create table test (a text, b text);
                    insert into test values ('a', 'b');
                    select test from test;`))
            .to.deep.equal([{ test: { a: 'a', b: 'b' } }]);
    })

    it.skip('can select record when aliased', () => {
        expect(many(`create table test (a text, b text);
                    insert into test values ('a', 'b');
                    select v from test as v;`))
            .to.deep.equal([{ v: { a: 'a', b: 'b' } }]);
    })

    it.skip('does not select parent alias record', () => {
        many(`create table test (a text, b text);
                    insert into test values ('a', 'b')`);
        assert.throws(() => many(`select test from test as v`), /does not exist/);
    })

    it.skip('does not select record when scoped', () => {
        many(`create table test (a text, b text);
                    insert into test values ('a', 'b')`);
        assert.throws(() => many(`select test.test from test`), /does not exist/);
    })

    it('can alias record selection', () => {
        expect(many(`create table test (a text, b text);
                    insert into test values ('a', 'b');
                    select test as v from test;`))
            .to.deep.equal([{ v: { a: 'a', b: 'b' } }]);
    })


    it('prefers column seleciton over record selection', () => {
        expect(many(`create table test (test text, b text);
                    insert into test values ('a', 'b');
                    select test from test;`))
            .to.deep.equal([{ test: 'a' }]);
    })


    it('does not leak null symbols (bugfix)', () => {
        expect(many(`
        create table test(data jsonb);
        insert into test values ('{"value": null}'), ('{"value":[null]}'), (null);
        select * from test;
        `)).to.deep.equal([
            { data: { value: null } },
            { data: { value: [null] } },
            { data: null },
        ])
    })



    it('can select raw values', () => {
        expect(many(`values (1), (2)`))
            .to.deep.equal([
                { column1: 1 },
                { column1: 2 },
            ]);
        expect(many(`values (1, 2)`))
            .to.deep.equal([
                { column1: 1, column2: 2 },
            ]);
    });

    it('cannot select raw values when not same length', () => {
        assert.throws(() => many(`values (1, 2), (3)`), /VALUES lists must all be the same length/);
    })


    it('can map column names 1', () => {
        expect(many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select * from test as xxx(a,b);
        `))
            .to.deep.equal([
                { a: 'id', b: 'name', value: 'value' },
            ]);
    });


    it('can map column names 2', () => {
        expect(many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select * from test newalias(a,b);
        `))
            .to.deep.equal([
                { a: 'id', b: 'name', value: 'value' },
            ]);
    });

    it('cannot map column names when too many names specified', () => {
        assert.throws(() => many(`
        create table test(id text, name text, value text);
        insert into test values ('id', 'name', 'value');
        select * from test newalias(a,b,c,d);
        `), /table "test" has 3 columns available but 4 columns specified/);
    });


    it('cannot use default in expression', () => {
        assert.throws(() => many(`values (1, default)`), /DEFAULT is not allowed in this context/);
    });



    describe.skip('Subqueries', () => {

        function mytable() {
            many(`CREATE TABLE my_table (id text NOT NULL PRIMARY KEY, name text NOT NULL, parent_id text);
            CREATE INDEX my_table_idx_name ON my_table (name);
            CREATE INDEX my_table_idx_id_parent_id ON my_table (id,parent_id);

            insert into my_table values ('parid', 'Parent', null);
            insert into my_table values ('childid', 'Child', 'parid');`);
        }

        it('fails if multiple columns in predicate', () => {
            mytable();
            assert.throws(() => many(`SELECT name FROM my_table as t1 WHERE id = (SELECT name, id FROM my_table as t2 WHERE t2.parent_id = t1.id);`), /subquery must return only one column/);
        });

        it('fails if multiple columns in selection', () => {
            mytable();
            assert.throws(() => many(`SELECT name, (SELECT name FROM my_table as t2 WHERE t2.parent_id = t1.id) FROM my_table as t1`), /subquery must return only one column/);
        });

        it('supports self aliasing (bugfix)', () => {
            mytable();
            expect(many(`SELECT name FROM my_table as t1 WHERE NOT EXISTS (SELECT * FROM my_table as t2 WHERE t2.parent_id = t1.id);`))
                .to.deep.equal([{ name: 'Child' }]);
        });



        // it('simplifies a subquery when possible', () => {
        //     mytable();
        //     let cnt = 0;
        //     db.on('subquery', () => {
        //         cnt++
        //     });
        //     db.on('non-constant-subquery', () => {
        //         assert.fail('Should not have raised non-constant-subquery');
        //     })
        //     expect(many(`SELECT name FROM my_table as t1 WHERE id = (SELECT id FROM my_table LIMIT 1)`))
        //         .to.deep.equal([{ name: 'Parent' }]);

        //     expect(cnt).to.equal(1, 'Was expecting subquery to be simplified');
        // })
    })




});