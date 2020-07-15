import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';

describe('Simple queries', () => {

    function simpleDb() {
        const db = newDb();
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
        const db = simpleDb();
        preventSeqScan(db);
        db.query.none(`insert into data(id) values ('some value')`);
        let got = db.query.many('select * from data where id="SOME ID"');
        expect(got).to.deep.equal([]);
        got = db.query.many('select * from data where id="some value"');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);

    });


    it('where constant true', () => {
        const db = simpleDb();
        db.query.none(`insert into data(id) values ('some value')`);
        let got = db.query.many('select * from data where 1 = 1');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);
    });

    it('where constant false', () => {
        const db = simpleDb();
        preventSeqScan(db);
        db.query.none(`insert into data(id) values ('some value')`);
        let got = db.query.many('select * from data where 1 = 0');
        expect(trimNullish(got)).to.deep.equal([]);
    });

    it('where on other', () => {
        const db = simpleDb();
        db.query.none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = db.query.many(`select * from data where str='none'`);
        expect(got).to.deep.equal([]);
        got = db.query.many(`select * from data where str='some str'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('can insert and select null', () => {
        const db = simpleDb();
        db.query.none(`insert into data(id, str) values ('some id', null)`);
        let got = db.query.many('select * from data where str is null');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id' }]);
        got = db.query.many('select * from data where str is not null');
        expect(got).to.deep.equal([]);
    });

    it('does not equate null values on seq scan', () => {
        const db = simpleDb();
        db.query.none(`insert into data(id, str, otherStr) values ('id1', null, null)`);
        db.query.none(`insert into data(id, str, otherStr) values ('id2', 'A', 'A')`);
        db.query.none(`insert into data(id, str, otherStr) values ('id3', 'A', 'B')`);
        db.query.none(`insert into data(id, str, otherStr) values ('id4', null, 'B')`);
        db.query.none(`insert into data(id, str, otherStr) values ('id5', 'A', null)`);
        const got = db.query.many('select * from data where str = otherStr');
        expect(got).to.deep.equal([{ id: 'id2', str: 'A', otherStr: 'A' }]);
    });

    function setupNulls() {
        const db = simpleDb()
        db.getTable('data')
            .createIndex(['str']);
        db.query.none(`insert into data(id, str) values ('id1', null)`);
        db.query.none(`insert into data(id, str) values ('id2', 'notnull2')`);
        db.query.none(`insert into data(id, str) values ('id3', null)`);
        db.query.none(`insert into data(id, str) values ('id4', 'notnull4')`);
        return db;
    }

    it('uses indexes for null values', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = db.query.many('select * from data where str is null');
        expect(got).to.deep.equal([{ id: 'id1', str: null }, { id: 'id3', str: null }]);
    });


    it('uses indexes for not null values', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = db.query.many('select * from data where str is not null');
        expect(got).to.deep.equal([{ id: 'id2', str: 'notnull2' }, { id: 'id4', str: 'notnull4' }]);
    });

    it('"IN" clause with constants and no index', () => {
        const db = simpleDb();
        db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
        const got = db.query.many(`select * from data where str in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
    });

    it('"IN" clause with constants index', () => {
        const db = simpleDb();
        db.getTable('data').createIndex(['str']);
        preventSeqScan(db);
        db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
        const got = db.query.many(`select * from data where str in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
    });

    it('"IN" clause with no constant', () => {
        const db = simpleDb();
        db.query.none(`insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')`);
        const got = db.query.many(`select * from data where id in (str, otherStr)`);
        expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
    });

    it('"IN" clause with constant value', () => {
        const db = simpleDb();
        db.query.none("insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')");
        const got = db.query.many(`select * from data where 'A' in (str, otherStr)`);
        expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
    });

    it('"NOT IN" clause with constants and no index', () => {
        const db = simpleDb();
        db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
        const got = db.query.many(`select * from data where str not in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
    });

    it('"NOT IN" clause with constants index', () => {
        const db = simpleDb();
        db.getTable('data').createIndex(['str']);
        preventSeqScan(db);
        db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
        const got = db.query.many(`select * from data where str not in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
    });


    it('AND query', () => {
        const db = simpleDb();
        preventSeqScan(db);
        db.query.none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = db.query.many(`select * from data where id='some id' AND str='other'`);
        expect(got).to.deep.equal([]);
        got = db.query.many(`select * from data where id='some id' and str='some str'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('OR query', () => {
        const db = simpleDb();
        db.query.none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = db.query.many(`select * from data where id='other' OR str='other'`);
        expect(got).to.deep.equal([]);
        got = db.query.many(`select * from data where id='some id' OR str='other'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
        got = db.query.many(`select * from data where id='some id' or str='some str'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });
});
