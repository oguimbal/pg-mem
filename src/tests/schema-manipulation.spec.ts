import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { watchUse } from '../utils';
import { preventSeqScan } from './test-utils';

describe('Schema manipulation', () => {

    it('table with primary', () => {
        const db = newDb();
        db.query.none(`create table test(id text primary key, value text)`);
        preventSeqScan(db, 'test');
        db.query.none(`insert into test(id, value) values ('A', 'Value A')`);
        const many = db.query.many(`select value from test where id='A'`);
        expect(many).to.deep.equal([{ value: 'Value A' }]);
    });

    it('table without primary', () => {
        const db = newDb();
        db.query.none(`create table test(value text)`);
        db.query.none(`insert into test(value) values ('Value A')`);
        const many = db.query.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 'Value A' }]);
    });

    it('table int', () => {
        const db = newDb();
        db.query.none(`create table test(value int)`);
        db.query.none(`insert into test(value) values (42.5)`);
        const many = db.query.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 43 }]); // must be rounded (int)
    });

    it('table integer', () => {
        const db = newDb();
        db.query.none(`create table test(value integer)`);
        db.query.none(`insert into test(value) values (42.5)`);
        const many = db.query.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 43 }]); // must be rounded (int)
    });

    it('table varchar', () => {
        const db = newDb();
        db.query.none(`create table test(value varchar)`);
        db.query.none(`insert into test(value) values ('test')`);
        const many = db.query.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 'test' }]);
    });

    it('table varchar(n)', () => {
        const db = newDb();
        db.query.none(`create table test(value varchar(5))`);
        db.query.none(`insert into test(value) values ('test')`);
        const many = db.query.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 'test' }]);
    });

    it('table varchar(n) with insert too long', () => {
        const db = newDb();
        db.query.none(`create table test(value varchar(5))`);
        assert.throws(() => {
            db.query.none(`insert into test(value) values ('12345678')`);
        });
    });


    it('table float', () => {
        const db = newDb();
        db.query.none(`create table test(value float)`);
        db.query.none(`insert into test(value) values (42.5)`);
        const many = db.query.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42.5 }]);
    });

    it('table decimal', () => {
        const db = newDb();
        db.query.none(`create table test(value decimal)`);
        db.query.none(`insert into test(value) values (42.5)`);
        const many = db.query.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42.5 }]);
    });
});
