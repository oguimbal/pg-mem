import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';

describe('pg-promise', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    function simpleDb() {
        db.public.none(`create table data(id text primary key, data jsonb, num integer, var varchar(10));
                        insert into data values ('str', '{"data": true}', 42, 'varchar')`);
    }


    it('can open connection', async () => {
        simpleDb();
        const pgp = db.adapters.createPgPromise();
        await pgp.connect();
        const got = await pgp.any('select * from data');
        assert.deepEqual(got, [{
            id: 'str',
            data: { data: true },
            num: 42,
            var: 'varchar',
        }]);
    });

    it('can execute begin', async () => {
        simpleDb();
        const pgp = db.adapters.createPgPromise();
        await pgp.connect();
        await pgp.any('begin');
    });
});