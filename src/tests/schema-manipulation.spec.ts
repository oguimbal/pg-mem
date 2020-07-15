import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { watchUse } from '../utils';
import { preventSeqScan } from './test-utils';

describe('Schema manipulation', () => {

    it('table with primary', async () => {
        const db = newDb();
        await db.query.none(`create table test(id text primary key, value text)`);
        preventSeqScan(db, 'test');
        await db.query.none(`insert into test(id, value) values ('A', 'Value A')`);
        const many = await db.query.many(`select value from test where id='A'`);
        expect(many).to.deep.equal([{ value: 'Value A' }]);
    });
});
