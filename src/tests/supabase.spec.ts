import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

// Exercises the primitives a default Supabase project relies on: uuid PKs via
// gen_random_uuid(), current_setting/set_config-backed JWT claims, auth.uid(), and RLS.
describe('Supabase defaults', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    it('gen_random_uuid() / uuid_generate_v4() produce distinct uuids', () => {
        const a = one(`select gen_random_uuid()::text as u`).u;
        const b = one(`select uuid_generate_v4()::text as u`).u;
        expect(a).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
        expect(a).not.toEqual(b);
    });

    it('CREATE EXTENSION for the Supabase default set is a no-op', () => {
        none(`create extension if not exists pgcrypto`);
        none(`create extension "uuid-ossp"`);
        none(`create extension if not exists pg_graphql`);
    });

    it('uuid primary key with gen_random_uuid() default', () => {
        none(`create table t (id uuid primary key default gen_random_uuid(), name text);
               insert into t(name) values ('a'), ('b')`);
        expect(one(`select count(*)::int as c from t`).c).toEqual(2);
        expect(new Set(many(`select id from t`).map(r => r.id)).size).toEqual(2);
    });

    it('set_config / current_setting round-trip (JWT claims)', () => {
        none(`select set_config('request.jwt.claims', '{"sub":"user-1"}', true)`);
        expect(one(`select current_setting('request.jwt.claims', true) as v`).v).toEqual('{"sub":"user-1"}');
        expect(one(`select (current_setting('request.jwt.claims', true)::json ->> 'sub') as v`).v).toEqual('user-1');
        // missing + missing_ok -> null; missing + strict -> error
        expect(one(`select current_setting('nope', true) as v`).v).toEqual(null);
        expectQueryError(() => one(`select current_setting('nope')`), /unrecognized configuration parameter/);
    });

    it('full RLS flow: auth.uid() from JWT claims filters rows per user', () => {
        none(`create extension if not exists pgcrypto;
               create schema auth;
               create function auth.uid() returns uuid as $$
                   select nullif(current_setting('request.jwt.claims', true)::json ->> 'sub', '')::uuid
               $$ language sql stable;
               create role anon;
               create role authenticated;
               create table todos (
                   id uuid primary key default gen_random_uuid(),
                   user_id uuid default auth.uid(),
                   title text
               );
               alter table todos enable row level security;
               create policy sel on todos for select to authenticated using (user_id = auth.uid());
               create policy ins on todos for insert to authenticated with check (user_id = auth.uid())`);

        // user 1
        none(`select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111"}', true)`);
        none(`set role authenticated`);
        none(`insert into todos(title) values ('buy milk'), ('walk dog')`);
        expect(many(`select title from todos order by title`).map(r => r.title)).toEqual(['buy milk', 'walk dog']);

        // user 2 sees only their own row
        none(`reset role`);
        none(`select set_config('request.jwt.claims', '{"sub":"22222222-2222-2222-2222-222222222222"}', true)`);
        none(`set role authenticated`);
        none(`insert into todos(title) values ('user2 task')`);
        expect(many(`select title from todos`).map(r => r.title)).toEqual(['user2 task']);

        // superuser bypasses RLS
        none(`reset role`);
        expect(one(`select count(*)::int as c from todos`).c).toEqual(3);
    });
});
