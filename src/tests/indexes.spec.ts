import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { preventSeqScan } from './test-utils';
import { Types } from '../datatypes';
import { _IDb } from '../interfaces-private';

describe('Indices', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'test') {
        return many(`select * from ${table}`);
    }
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

    function setupNulls(withIndex = true) {
        db = simpleDb()
        if (withIndex) {
            none('create index on data(str);')
        }
        none(`insert into data(id, str) values ('id1', null);
                insert into data(id, str) values ('id2', 'notnull2');
                insert into data(id, str) values ('id3', null);
                insert into data(id, str) values ('id4', 'notnull4')`);
        return db;
    }

    it('uses indexes for null values', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = many('select * from data where str is null');
        expect(got).to.deep.equal([{ id: 'id1', str: null, otherstr: null }, { id: 'id3', str: null, otherstr: null }]);
    });

    it('returns something on seqscan for is null', () => {
        const db = setupNulls(false);
        const got = many('select * from data where str is null');
        expect(got).to.deep.equal([{ id: 'id1', str: null, otherstr: null }, { id: 'id3', str: null, otherstr: null }]);
    });


    it('uses indexes for not null values', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = many('select id, str from data where str is not null');
        expect(got).to.deep.equal([{ id: 'id2', str: 'notnull2' }, { id: 'id4', str: 'notnull4' }]);
    });

    it('primary index does not allow duplicates', () => {
        none(`create table test(id text primary key);
                insert into test values ('id1');`);
        assert.throws(() => none(`insert into test values ('id1')`));
        expect(all().map(x => x.id)).to.deep.equal(['id1']);
    });


    it('primary index does not allow null values', () => {
        none(`create table test(id text primary key);`);
        assert.throws(() => none(`insert into test values (null)`));
    });


    it('can unique index', () => {
        none(`create table test(col text);
            create unique index idx1 on test(col);
            insert into test values ('one'), ('two')`);

        assert.throws(() => none(`insert into test values ('one')`), /constraint/);
    });

    it('can create partial indexes', () => {

        // https://github.com/oguimbal/pg-mem/issues/89
        none(`create table my_table(col1 text, col2 text);
            CREATE UNIQUE INDEX my_idx ON my_table (col1, (col2 IS NULL)) WHERE col2 IS NULL;
            insert into my_table values ('a', 'a'), ('a', 'b'), ('a', null), ('b', null)`);

        assert.throws(() => none(`insert into my_table values ('a', null)`), /constraint/);
    });


    describe('[bugfix #160] indexes with default values', () => {
        // checks issue described in https://github.com/oguimbal/pg-mem/issues/160
        function begin160(condition = 'deleted_at IS NULL') {
            none(`create table "test_table" (
                "id" character varying(36) NOT NULL,
                "unique_data" character varying(36),
                "deleted_at" timestamp without time zone,
                CONSTRAINT "PK_test_table_id"
                PRIMARY KEY ("id")
              );

              CREATE UNIQUE INDEX "UXtest_table_unique_data" ON "public"."test_table" ("unique_data") WHERE ${condition};

              insert into test_table ("id", "unique_data", "deleted_at") VALUES('1', default, default );`);
        }

        function simple160(condition = 'deleted_at IS NULL') {
            begin160(condition);

            // this was throwing:
            none(`insert into test_table ("id", "unique_data", "deleted_at") VALUES('2', default, default );`)
        }

        it('can insert multiple default values on partial indexes ', () => {
            simple160();
        });

        it('can update to default value on partial indexes ', () => {
            // checks issue described in https://github.com/oguimbal/pg-mem/issues/160
            begin160();
            none(`insert into test_table ("id", "unique_data", "deleted_at") VALUES('2', 'x', default );`);


            // this was throwing:
            none(`update test_table set unique_data=default where id='2'`)
        });

        it('insert into select is OK with partial indexes', () => {
            // checks issue described in https://github.com/oguimbal/pg-mem/issues/160
            begin160();

            // was throwing n°1
            none(`insert into test_table ("id", "unique_data", "deleted_at") VALUES('2', default, default );`)

            // was throwing n°2
            none(`insert into test_table(id,unique_data,deleted_at) (select id || 'bis', unique_data, deleted_at from test_table)`);
        });


        it('behaves the same on notnulls', () => {
            simple160('deleted_at NOTNULL');
        });

        it('behaves the same on not isnull', () => {
            simple160('NOT(deleted_at ISNULL)');
        });

        it('behaves the same on not notnull', () => {
            simple160('NOT(deleted_at NOTNULL)');
        });

        it('throws when unique is not default', () => {
            none(`
            drop table if exists test_table;
            create table "test_table" (
              "id" character varying(36) NOT NULL,
              "unique_data" character varying(36),
              "deleted_at" timestamp without time zone,
              CONSTRAINT "PK_test_table_id"
              PRIMARY KEY ("id")
            );

            CREATE UNIQUE INDEX "UXtest_table_unique_data" ON "public"."test_table" ("unique_data") where deleted_at isnull;

            insert into test_table ("id", "unique_data", "deleted_at") VALUES('1', 'x', default );
            `)

            assert.throws(() => none(`insert into test_table ("id", "unique_data", "deleted_at") VALUES('2', 'x', default );`), /duplicate key value violates unique constraint/);
        });


        it('accepts two default not set values on non partial', () => {
            none(`
                        create table "test_table" (
                        "id" text not null primary key,
                        "unique_data" text
                        );

                        CREATE UNIQUE INDEX ON test_table (unique_data);

                        insert into test_table ("id", "unique_data") VALUES('1', default);
                        insert into test_table ("id", "unique_data") VALUES('2', default);
                `);
        })


        it('accepts two default null values on non partial', () => {
            none(`
                    create table "test_table" (
                    "id" text not null primary key,
                    "unique_data" text default null
                    );

                    CREATE UNIQUE INDEX ON test_table (unique_data);

                    insert into test_table ("id", "unique_data") VALUES('1', default);
                    insert into test_table ("id", "unique_data") VALUES('2', default);
                `);
        });


        it('rejects two default not null values on non partial', () => {
            none(`
                        create table "test_table" (
                        "id" text not null primary key,
                        "unique_data" text default 'some default'
                        );

                        CREATE UNIQUE INDEX ON test_table (unique_data);

                        insert into test_table ("id", "unique_data") VALUES('1', default);
                `);

            assert.throws(() => none(`insert into test_table ("id", "unique_data") VALUES('2', default)`), /duplicate key value violates unique constraint/);
        });

        it('allows multiple null values in unique index', () => {
            none(`
                    create table "test_table" (
                    "id" text not null primary key,
                    "unique_data" text default null
                    );

                    CREATE UNIQUE INDEX ON test_table (unique_data);

                    insert into test_table ("id", "unique_data") VALUES('1', default);
                    insert into test_table ("id", "unique_data") VALUES('2', default);
                    insert into test_table ("id", "unique_data") VALUES('3', null);
                    insert into test_table ("id", "unique_data") VALUES('4', null);
                                    `);
        });

        it('prevens inserting nulls on compound primary keys even when defaults', () => {
            none(`
            create table "test_table" (
             "u1" text default null,
             "u2" text default null,
             primary key (u1, u2)
            );`);

            assert.throws(() => none(`insert into test_table ("u1", "u2") VALUES('1', default)`), /\s+null\s+/);
            assert.throws(() => none(`insert into test_table ("u1", "u2") VALUES('1', null)`), /\s+null\s+/);
        });
    });

    it('can create the same named index twice', () => {
        none(`create table test(col text);
            create index idx1 on test(col);
            create index idx2 on test(col);`);
    });


    it('can create index if not exists', () => {
        none(`create table test(col text);
            create index idxname on test(col);
            create index if not exists idxname on test(col);`);
    });


    it('reads .ifNotExists when creating index', () => {
        none(`create table test(col text);
            create index if not exists idxname on test(col);`);
    });

    it('cannot create index twice', () => {
        none(`create table test(col text);
            create index idxname on test(col);`);
        assert.throws(() => none(`create index idxname on test(col)`), /exists/);
    });

    it('cannot create index which has same name as a table', () => {
        none(`create table test(col text);`);
        assert.throws(() => none(`create index test on test(col)`), /exists/);
    });


    it('can create the same anonymous index twice', () => {
        none(`create table test(col text);
            create index on test(col);
            create index on test(col);`);
    });


    it('unique index does not allow duplicates', () => {
        none(`create table test(id text primary key, val text unique);
                insert into test values ('id1', 'A');`);
        assert.throws(() => none(`insert into test values ('id2', 'A')`));
        expect(all().map(x => x.id)).to.deep.equal(['id1']);
    });


    it('index allows duplicates', () => {
        none(`create table test(id text primary key, val text);
                create index on test(val);
                insert into test values ('id1', 'A');
                insert into test values ('id2', 'B');
                insert into test values ('id3', 'A');`);
        expect(all().map(x => x.id)).to.deep.equal(['id1', 'id2', 'id3']);
        preventSeqScan(db); // <== should use index even if index is on expression
        expect(many(`select id from test where val='A'`).map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });

    it('can create index on an expression', () => {
        none(`create table test(id text primary key, val text);
                create index on test(LOWER(val));
                insert into test values ('id1', 'A');
                insert into test values ('id2', 'B');
                insert into test values ('id3', 'a');`);
        preventSeqScan(db); // <== should use index even if index is on expression
        expect(many(`select id from test where lower(val)='a'`).map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });


    it('can commutative index on an expression', () => {
        none(`create table test(a integer, b integer);
                create index on test((a+b));
                insert into test values (1, 2);
                insert into test values (3, 4);`);
        preventSeqScan(db); // <== should use index even if index is on expression
        // notice that b+a is not the expression usedin index creation
        expect(many(`select a from test where (b+a)=3`).map(x => x.a)).to.deep.equal([1]);
    });


    it('can use an index on an aliased selection not aliased var', () => {
        preventSeqScan(db);
        const got = many(`create table test(txt text, val integer);
        create index on test(txt);
        create index on test(val);
        insert into test values ('A', 999);
        insert into test values ('A', 0);
        insert into test values ('A', 1);
        insert into test values ('B', 2);
        insert into test values ('C', 3);
        select * from (select val from test where txt != 'A') x where x.val > 1`);

        const explain = db.public.explainLastSelect();
        // assert.deepEqual(explain, {} as any);
        assert.deepEqual(explain, {
            _: 'ineq',
            entropy: 3,
            id: 1,
            on: {
                _: 'indexMap',
                of: {
                    _: 'indexRestriction',
                    lookup: {
                        _: 'btree',
                        btree: ['val'],
                        onTable: 'test',
                    },
                    for: {
                        _: 'neq',
                        entropy: 10 / 3,
                        id: 2,
                        on: {
                            _: 'btree',
                            btree: ['txt'],
                            onTable: 'test',
                        }
                    }
                }
            }
        })

        expect(got)
            .to.deep.equal([{ val: 2 }, { val: 3 }]);
    });

    it('can use an index on an aliased selection & aliased var', () => {
        // preventSeqScan(db);
        const got = many(`create table test(txt text, val integer);
        create index on test(txt);
        create index on test(val);
        insert into test values ('A', 999);
        insert into test values ('A', 0);
        insert into test values ('A', 1);
        insert into test values ('B', 2);
        insert into test values ('C', 3);
        select * from (select val as xx from test where txt != 'A') x where x.xx > 1`);

        const explain = db.public.explainSelect(`select * from (select val as xx from test where txt != 'A') x where x.xx > 1`);
        assert.deepEqual(explain, {
            _: 'seqFilter',
            id: 1,
            filtered: {
                _: 'map',
                id: 2,
                select: [{
                    what: {
                        on: 'test',
                        col: 'val'
                    },
                    as: 'xx',
                }],
                of: {
                    _: 'neq',
                    id: 3,
                    entropy: 10 / 3,
                    on: {
                        _: 'btree',
                        btree: ['txt'],
                        onTable: 'test',
                    }
                }
            }
        });

        expect(got)
            .to.deep.equal([{ xx: 2 }, { xx: 3 }]);
    });
    it('can use an index on an aliased "!=" selection', () => {
        // preventSeqScan(db);
        expect(many(`create table test(txt text, val integer);
                create index on test(txt);
                create index on test(val);
                insert into test values ('A', 999);
                insert into test values ('A', 0);
                insert into test values ('A', 1);
                insert into test values ('B', 2);
                insert into test values ('C', 3);
                select * from (select val as xx from test where txt != 'A') x where x.xx > 1`))
            .to.deep.equal([{ xx: 2 }, { xx: 3 }]);
    });

    it('can use an index on an aliased "=" selection', () => {
        // preventSeqScan(db);
        expect(many(`create table test(txt text, val integer);
                create index on test(txt);
                create index on test(val);
                insert into test values ('A', 999);
                insert into test values ('A', 0);
                insert into test values ('A', 1);
                insert into test values ('B', 2);
                insert into test values ('C', 3);
                select * from (select val as xx from test where txt = 'A') x where x.xx >= 1`))
            .to.deep.equal([{ xx: 999 }, { xx: 1 }]);
    });

    it('can use an index on an aliased "=" expression selection', () => {
        // preventSeqScan(db);
        expect(many(`create table test(txt text, val integer);
                create index on test(lower(txt));
                create index on test(val);
                insert into test values ('A', 999);
                insert into test values ('A', 0);
                insert into test values ('A', 1);
                insert into test values ('B', 2);
                insert into test values ('C', 3);
                select * from (select val as xx from test where lower(txt) = 'a') x where x.xx >= 1`))
            .to.deep.equal([{ xx: 999 }, { xx: 1 }]);
    });


    it('can use an index expression on a transformedselection', () => {
        // preventSeqScan(db);
        expect(many(`create table test(txt text, val integer);
                create index on test(lower(txt));
                create index on test(val);
                insert into test values ('A', 999);
                insert into test values ('A', 0);
                insert into test values ('A', 1);
                insert into test values ('B', 2);
                insert into test values ('C', 3);
                select valx from (select val as valx, txt as txtx from test where val >= 1) x where lower(x.txtx) = 'a'`))
            .to.deep.equal([{ valx: 1 }, { valx: 999 }]);
    });


    it('can use constant in index expressions', () => {
        none(`create table test(id text primary key, val text);
                create index on test(concat(val, 'X'));
                insert into test values ('id1', 'A');
                insert into test values ('id2', 'B');
                insert into test values ('id3', 'A');`);
        preventSeqScan(db); // <== should use index even if index is on expression
        expect(many(`select id from test where concat(val, 'X')='AX'`).map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });

    it('can use constant in index expressions bis', () => {
        none(`create table test(id text primary key, a int, b int);
                create index on test((a+b));
                insert into test values ('id1', 40, 2);
                insert into test values ('id2', 1, 2);
                insert into test values ('id3', 30, 12);`);
        preventSeqScan(db); // <== should use index even if index is on expression
        expect(many(`select id from test where a+b=42`).map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });

    it('can use implicit cast index on index', () => {
        expect(many(`create table example(id int primary key);
                insert into example(id) values (1);
                select * from example where id='1';`))
            .to.deep.equal([
                { id: 1 }
            ]);
        expect(many(`select * from example where id>'0';`))
            .to.deep.equal([
                { id: 1 }
            ]);
        expect(many(`select * from example where id<'3';`))
            .to.deep.equal([
                { id: 1 }
            ]);
        expect(many(`select * from example where id>'1';`))
            .to.deep.equal([]);
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
});
