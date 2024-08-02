import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';
import { DatabasePool, sql } from 'slonik';
import { z } from 'zod';

describe('slonik', () => {

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


    it('can open select without arg', async () => {
        simpleDb();
        const pool = await db.adapters.createSlonik() as DatabasePool;
        await pool.connect(async (connection) => {
            const got = await connection.query(sql.unsafe`select * from data`);
            expect(got.rows).toEqual([{
                id: 'str',
                data: { data: true } as any,
                num: 42,
                var: 'varchar',
            }]);
        });
    });

    it('can open select with arg', async () => {
        simpleDb();
        const pool = await db.adapters.createSlonik() as DatabasePool;
        await pool.connect(async (connection) => {
            const str = 'str';
            const got = await connection.query(sql.unsafe`select * from data where id=${str}`);
            expect(got.rows).toEqual([{
                id: 'str',
                data: { data: true } as any,
                num: 42,
                var: 'varchar',
            }]);
        });
    });

    it('can select bigint (i.e. it runs zod coerce)', async () => {
        const pool = await db.adapters.createSlonik({ zodValidation: true }) as DatabasePool;
        await pool.connect(async (connection) => {
            const got = await connection.query(sql.type(z.object({ value: z.coerce.bigint() }))`select 234218741::bigint as value`);
            expect(got.rows).toEqual([{ value: BigInt(234218741) as any }]);
        });
    });
});
