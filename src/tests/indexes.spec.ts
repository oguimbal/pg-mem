import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';

describe('Indices', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'test') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.query.many.bind(db.query);
        none = db.query.none.bind(db.query);
    });

    it ('primary index does not allow duplicates', () => {
        none(`create table test(id text primary key);
                insert into test values ('id1');`);
        assert.throws(() => none(`insert into test values ('id1')`));
        expect(all().map(x => x.id)).to.deep.equal(['id1']);
    });


    it ('primary index does not allow null values', () => {
        none(`create table test(id text primary key);`);
        assert.throws(() => none(`insert into test values (null)`));
    });


    it ('unique index does not allow duplicates', () => {
        none(`create table test(id text primary key, val text unique);
                insert into test values ('id1', 'A');`);
        assert.throws(() => none(`insert into test values ('id2', 'A')`));
        expect(all().map(x => x.id)).to.deep.equal(['id1']);
    });


    it ('index allows duplicates', () => {
        none(`create table test(id text primary key, val text);
                create index on test(val);
                insert into test values ('id1', 'A');
                insert into test values ('id2', 'B');
                insert into test values ('id3', 'A');`);
        expect(all().map(x => x.id)).to.deep.equal(['id1', 'id2', 'id3']);
        preventSeqScan(db); // <== should use index even if index is on expression
        expect(many(`select id from test where val='A'`).map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });

    it ('can create index on an expression', () => {
        none(`create table test(id text primary key, val text);
                create index on test(LOWER(val));
                insert into test values ('id1', 'A');
                insert into test values ('id2', 'B');
                insert into test values ('id3', 'a');`);
        preventSeqScan(db); // <== should use index even if index is on expression
        expect(many(`select id from test where lower(val)='a'`).map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });

    it ('can use constant in index expressions', () => {
        none(`create table test(id text primary key, val text);
                create index on test(concat(val, 'X'));
                insert into test values ('id1', 'A');
                insert into test values ('id2', 'B');
                insert into test values ('id3', 'A');`);
        preventSeqScan(db); // <== should use index even if index is on expression
        expect(many(`select id from test where concat(val, 'X')='AX'`).map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });

    it ('can use constant in index expressions bis', () => {
        none(`create table test(id text primary key, a int, b int);
                create index on test((a+b));
                insert into test values ('id1', 40, 2);
                insert into test values ('id2', 1, 2);
                insert into test values ('id3', 30, 12);`);
        preventSeqScan(db); // <== should use index even if index is on expression
        expect(many(`select id from test where a+b=42`).map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });
});
