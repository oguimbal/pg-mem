import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

// Behaviours here were verified against a real postgres 16.

describe('RLS enforcement', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    const ids = (sql: string) => many(sql).map(r => r.id).sort((a, b) => a - b);
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        none(`create table docs (id int, owner text, body text)`);
        none(`insert into docs values (1,'alice','a'),(2,'bob','b'),(3,'alice','c')`);
        none(`create role alice nologin`);
        none(`create role bob nologin`);
        none(`alter table docs enable row level security`);
    });

    describe('select visibility', () => {
        beforeEach(() => none(`create policy p on docs for select using (owner = current_user)`));

        it('filters rows by the current role', () => {
            expect(ids(`select id from docs`)).toEqual([1, 2, 3]); // superuser bypasses
            none(`set role alice`);
            expect(ids(`select id from docs`)).toEqual([1, 3]);
            none(`set role bob`);
            expect(ids(`select id from docs`)).toEqual([2]);
        });

        it('re-evaluates per role even on a repeated query (no stale plan)', () => {
            none(`set role alice`);
            expect(ids(`select id from docs`)).toEqual([1, 3]);
            none(`set role bob`);
            // same SQL string, different role -> must not reuse alice's result
            expect(ids(`select id from docs`)).toEqual([2]);
        });

        it('works through a WHERE clause', () => {
            none(`set role alice`);
            expect(ids(`select id from docs where body = 'c'`)).toEqual([3]);
        });
    });

    it('default-denies when RLS is on with no applicable policy', () => {
        none(`set role alice`);
        expect(many(`select id from docs`)).toEqual([]);
        none(`reset role`);
        expect(ids(`select id from docs`)).toEqual([1, 2, 3]); // superuser still sees all
    });

    describe('write checks', () => {
        beforeEach(() => none(`create policy p on docs for all
                                using (owner = current_user)
                                with check (owner = current_user)`));

        it('allows inserting own rows, blocks others (WITH CHECK)', () => {
            none(`set role alice`);
            none(`insert into docs values (4,'alice','d')`);
            expect(ids(`select id from docs`)).toEqual([1, 3, 4]);
            expectQueryError(() => none(`insert into docs values (5,'bob','x')`),
                /new row violates row-level security policy for table "docs"/);
        });

        it('update only affects visible rows and enforces WITH CHECK', () => {
            none(`set role alice`);
            // invisible row: no-op, no error
            none(`update docs set body = 'hack' where id = 2`);
            // reassigning ownership away violates WITH CHECK
            expectQueryError(() => none(`update docs set owner = 'bob' where id = 1`),
                /violates row-level security policy/);
            none(`reset role`);
            expect(many(`select body from docs where id = 2`)[0].body).toBe('b');
        });

        it('delete only affects visible rows', () => {
            none(`set role alice`);
            none(`delete from docs where id = 2`); // bob's row, invisible -> no-op
            none(`delete from docs where id = 1`); // own row -> deleted
            none(`reset role`);
            expect(ids(`select id from docs`)).toEqual([2, 3]);
        });
    });

    describe('bypass', () => {
        beforeEach(() => none(`create policy p on docs for select using (owner = current_user)`));

        it('BYPASSRLS roles see everything', () => {
            none(`create role admin nologin bypassrls`);
            none(`set role admin`);
            expect(ids(`select id from docs`)).toEqual([1, 2, 3]);
        });

        it('superuser roles see everything', () => {
            none(`create role root nologin superuser`);
            none(`set role root`);
            expect(ids(`select id from docs`)).toEqual([1, 2, 3]);
        });
    });

    describe('permissive vs restrictive', () => {
        it('permissive policies are OR-combined', () => {
            none(`create policy p1 on docs for select using (owner = current_user)`);
            none(`create policy p2 on docs for select using (body = 'b')`);
            none(`set role alice`);
            // alice's own rows (1,3) OR body='b' (2)
            expect(ids(`select id from docs`)).toEqual([1, 2, 3]);
        });

        it('restrictive policies are AND-combined', () => {
            none(`create policy p1 on docs for select using (owner = current_user)`);
            none(`create policy p2 on docs as restrictive for select using (body = 'c')`);
            none(`set role alice`);
            // alice's rows (1,3) AND body='c' -> only 3
            expect(ids(`select id from docs`)).toEqual([3]);
        });
    });
});
