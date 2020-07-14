import 'mocha';
import 'chai';
import { newDb } from './db';
import { DataType, IMemoryDb } from './interfaces';
import { expect, assert } from 'chai';
import { trimNullish } from './utils';
import { Parser } from 'node-sql-parser';

describe('DB', () => {

    function simpleDb() {
        const db = newDb();
        db.declareTable({
            name: 'data',
            fields: [{
                id: 'id',
                type: DataType.text,
                primary: true,
            }, {
                id: 'str',
                type: DataType.text,
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
        let got = await db.query.many('select * from data where id="some id"');
        expect(got).to.deep.equal([{ id: 'some id', str: null }]);
        got = await db.query.many('select * from data where str is null');
        expect(got).to.deep.equal([{ id: 'some id', str: null }]);
        got = await db.query.many('select * from data where str is not null');
        expect(got).to.deep.equal([]);
    });



    it('"IN" clause with constants', async () => {
        const db = simpleDb();
        await db.query.none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
        const got = await db.query.many(`select * from data where str in ('str1', 'str3')`);
        expect(got).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
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
