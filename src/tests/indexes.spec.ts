import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';
import { Types } from '../datatypes';
import { _IDb } from '../interfaces-private';

describe('[Queries] Indices', () => {

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
                constraint: { type: 'primary key' },
            }, {
                name: 'str',
                type: Types.text(),
            }, {
                name: 'otherStr',
                type: Types.text(),
            }],
        });
        return db;
    }

    function setupNulls() {
        const db = simpleDb()
        none('create index on data(str)');
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
        expect(got).to.deep.equal([{ id: 'id1', str: null, otherStr: null }, { id: 'id3', str: null, otherStr: null }]);
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


    it('can use an index on an aliased selection', () => {
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

        const explain = db.public.explainSelect(`select * from (select val from test where txt != 'A') x where x.val > 1`);
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
                        entropy: 10/3,
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
