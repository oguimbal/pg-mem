import 'mocha';
import 'chai';
import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';

describe('DB', () => {

    function simpleDb() {
        const db = newDb();
        db.declareTable({
            name: 'data',
            fields: [{
                id: 'id',
                type: Types.text,
                primary: true,
            }, {
                id: 'str',
                type: Types.text,
            }, {
                id: 'otherStr',
                type: Types.text,
            }],
        });
        return db;
    }

    function preventSeqScan(db: IMemoryDb, table = 'data') {
        db.getTable(table).on('seq-scan', () => {
            assert.fail('Should have used index');
        });
    }

    it('where on primary', async () => {
        const db = simpleDb();
        preventSeqScan(db);
        await db.query.none(`insert into data(id) values ('some value')`);
        let got = await db.query.many('select * from data where id="SOME ID"');
        expect(got).to.deep.equal([]);
        got = await db.query.many('select * from data where id="some value"');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);

    });

    it('where on other', async () => {
        const db = simpleDb();
        await db.query.none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = await db.query.many(`select * from data where str='none'`);
        expect(got).to.deep.equal([]);
        got = await db.query.many(`select * from data where str='some str'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('can insert and select null', async () => {
        const db = simpleDb();
        await db.query.none(`insert into data(id, str) values ('some id', null)`);
        let got = await db.query.many('select * from data where str is null');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id' }]);
        got = await db.query.many('select * from data where str is not null');
        expect(got).to.deep.equal([]);
    });

    it('does not equate null values on seq scan', async () => {
        const db = simpleDb();
        await db.query.none(`insert into data(id, str, otherStr) values ('id1', null, null)`);
        await db.query.none(`insert into data(id, str, otherStr) values ('id2', 'A', 'A')`);
        await db.query.none(`insert into data(id, str, otherStr) values ('id3', 'A', 'B')`);
        await db.query.none(`insert into data(id, str, otherStr) values ('id4', null, 'B')`);
        await db.query.none(`insert into data(id, str, otherStr) values ('id5', 'A', null)`);
        const got = await db.query.many('select * from data where str = otherStr');
        expect(got).to.deep.equal([{ id: 'id2', str: 'A', otherStr: 'A' }]);
    });

    async function setupNulls() {
        const db = simpleDb()
        db.getTable('data')
            .createIndex(['str']);
        await db.query.none(`insert into data(id, str) values ('id1', null)`);
        await db.query.none(`insert into data(id, str) values ('id2', 'notnull2')`);
        await db.query.none(`insert into data(id, str) values ('id3', null)`);
        await db.query.none(`insert into data(id, str) values ('id4', 'notnull4')`);
        return db;
    }

    it('uses indexes for null values', async () => {
        const db = await setupNulls();
        preventSeqScan(db);
        const got = await db.query.many('select * from data where str is null');
        expect(got).to.deep.equal([{ id: 'id1', str: null }, { id: 'id3', str: null }]);
    });


    it('uses indexes for not null values', async () => {
        const db = await setupNulls();
        preventSeqScan(db);
        const got = await db.query.many('select * from data where str is not null');
        expect(got).to.deep.equal([{ id: 'id2', str: 'notnull2' }, { id: 'id4', str: 'notnull4' }]);
    });

    it('"IN" clause with constants and no index', async () => {
        const db = simpleDb();
        await db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
        const got = await db.query.many(`select * from data where str in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
    });

    it('"IN" clause with constants index', async () => {
        const db = simpleDb();
        db.getTable('data').createIndex(['str']);
        preventSeqScan(db);
        await db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
        const got = await db.query.many(`select * from data where str in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
    });

    it('"IN" clause with no constant', async () => {
        const db = simpleDb();
        await db.query.none(`insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')`);
        const got = await db.query.many(`select * from data where id in (str, otherStr)`);
        expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
    });

    it('"IN" clause with constant value', async () => {
        const db = simpleDb();
        await db.query.none("insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')");
        const got = await db.query.many(`select * from data where 'A' in (str, otherStr)`);
        expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
    });

    it('"NOT IN" clause with constants and no index', async () => {
        const db = simpleDb();
        await db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
        const got = await db.query.many(`select * from data where str not in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
    });

    it('"NOT IN" clause with constants index', async () => {
        const db = simpleDb();
        db.getTable('data').createIndex(['str']);
        preventSeqScan(db);
        await db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
        const got = await db.query.many(`select * from data where str not in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
    });


    it('AND query', async () => {
        const db = simpleDb();
        preventSeqScan(db);
        await db.query.none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = await db.query.many(`select * from data where id='some id' AND str='other'`);
        expect(got).to.deep.equal([]);
        got = await db.query.many(`select * from data where id='some id' and str='some str'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('OR query', async () => {
        const db = simpleDb();
        await db.query.none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = await db.query.many(`select * from data where id='other' OR str='other'`);
        expect(got).to.deep.equal([]);
        got = await db.query.many(`select * from data where id='some id' OR str='other'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
        got = await db.query.many(`select * from data where id='some id' or str='some str'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });
});
