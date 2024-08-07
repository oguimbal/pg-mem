import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';

describe('pg', () => {

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

    it('can select without arg', async () => {
        simpleDb();
        const { Client } = db.adapters.createPg();
        const client = new Client();
        await client.connect();
        const got = await client.query('select * from data');
        expect(got.rows).toEqual([{
            id: 'str',
            data: { data: true },
            num: 42,
            var: 'varchar',
        }]);
        await client.end();
    });

    it('can select with arg', async () => {
        simpleDb();
        const { Client } = db.adapters.createPg();
        const client = new Client();
        await client.connect();
        const got = await client.query('select * from data where id = $1', ['str']);
        expect(got.rows).toEqual([{
            id: 'str',
            data: { data: true },
            num: 42,
            var: 'varchar',
        }]);
        await client.end();
    });

    it('can select with callback', (done) => {
        simpleDb();
        const { Client } = db.adapters.createPg();
        const client = new Client();
        client.connect();
        client.query('select * from data where id = $1', ['str'], (err: any, res: any) => {
            expect(res.rows).toEqual([{
                id: 'str',
                data: { data: true },
                num: 42,
                var: 'varchar',
            }]);
            client.end();
            done();
        });
    });

    it('handles any($1)', async () => {
        simpleDb();
        const { Client } = db.adapters.createPg();
        const client = new Client();
        await client.connect();
        many(`create table dispute (id text);
                insert into dispute values ('A'), ('B'), ('OTHER')`);
        const got = await client.query('select * from dispute where id = ANY($1)', [['A', 'B']]);
        expect(got.rows).toEqual([{ id: 'A' }, { id: 'B' }]);
        await client.end();
    });

    it('handles empty arrays in prepared statements', async () => {
        simpleDb();
        const { Client } = db.adapters.createPg();
        const client = new Client();
        await client.connect();
        many('create table mytable (items varchar[]);');
        await client.query('insert into mytable (items) values ($1);', [[]]);
        const got = await client.query('select * from mytable;');
        expect(got.rows).toEqual([{ items: [] }]);
        await client.end();
    });

    it('does not leak symbol properties in rows', async () => {
        simpleDb();
        const { Client } = db.adapters.createPg();
        const client = new Client();
        await client.connect();
        many(`create table mytable (msg varchar);
            insert into mytable values ('hello'), ('hi');`);
        const got = await client.query('select * from mytable;');
        expect(Object.getOwnPropertySymbols(got.rows[0]).length).toBe(0);
        await client.end();
    });


    it('supports .toPosgres() on query args', async () => {
        // see
        simpleDb();
        const { Client } = db.adapters.createPg();
        const client = new Client();
        await client.connect();
        const complexValue = {
            toPostgres() {
                return 'str'
            },
        };
        const got = await client.query('select * from data where id = $1', [complexValue]);
        expect(got.rows).toEqual([{
            id: 'str',
            data: { data: true },
            num: 42,
            var: 'varchar',
        }]);
        await client.end();
    });
});
