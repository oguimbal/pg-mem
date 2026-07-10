import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('Roles & session identity', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('defaults to the pg_mem superuser role', () => {
        expect(one(`select current_user as u, session_user as s, current_role as r`))
            .toEqual({ u: 'pg_mem', s: 'pg_mem', r: 'pg_mem' });
    });

    it('SET ROLE changes current_user but not session_user', () => {
        none(`create role app_user nologin`);
        none(`set role app_user`);
        expect(one(`select current_user as u, session_user as s`))
            .toEqual({ u: 'app_user', s: 'pg_mem' });
    });

    it('RESET ROLE and SET ROLE NONE return to the session role', () => {
        none(`create role app_user nologin`);
        none(`set role app_user`);
        none(`reset role`);
        expect(one(`select current_user as u`).u).toBe('pg_mem');
        none(`set role app_user`);
        none(`set role none`);
        expect(one(`select current_user as u`).u).toBe('pg_mem');
    });

    it('CREATE USER implies login', () => {
        none(`create user alice`);
        // parses & stores; no behavioral assertion beyond existing
        none(`set role alice`);
        expect(one(`select current_user as u`).u).toBe('alice');
    });

    it('errors on unknown / duplicate / missing roles', () => {
        expectQueryError(() => none(`set role ghost`), /role "ghost" does not exist/);
        none(`create role dup nologin`);
        expectQueryError(() => none(`create role dup`), /role "dup" already exists/);
        expectQueryError(() => none(`drop role ghost`), /role "ghost" does not exist/);
        none(`drop role if exists ghost`); // no throw
    });

    it('drop role removes it', () => {
        none(`create role tmp nologin`);
        none(`drop role tmp`);
        expectQueryError(() => none(`set role tmp`), /role "tmp" does not exist/);
    });

    it('roles and role changes roll back within a transaction batch', () => {
        const r = many(`begin;
                        create role tmp nologin;
                        set role tmp;
                        rollback;
                        select current_user as u`);
        expect(r[r.length - 1]).toEqual({ u: 'pg_mem' });
    });
});
