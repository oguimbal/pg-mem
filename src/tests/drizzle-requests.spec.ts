import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';

describe('drizzle - requests', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb({
            autoCreateForeignKeyIndices: true,
        }) as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    function simpleDb() {
        db.public.none('create table data(id text primary key, data jsonb, num integer, var varchar(10))');
    }

    it('can select pg_user', () => {
        simpleDb();
        many(`select s.nspname as table_schema
            from pg_catalog.pg_namespace s
            join pg_catalog.pg_user u on u.usesysid = s.nspowner
            where nspname not in ('information_schema', 'pg_catalog', 'public')
                and nspname not like 'pg_toast%'
                and nspname not like 'pg_temp_%'
            order by table_schema`);
    });

    it('can select pg_sequences', () => {
        simpleDb();
        many(`select schemaname, sequencename, start_value, min_value, max_value, increment_by, cycle, cache_size from pg_sequences as seq WHERE schemaname = 'public'`);
    });

    it('can select pg_enum', () => {
        simpleDb();
        many(`select n.nspname as enum_schema,
            t.typname as enum_name,
            e.enumlabel as enum_value,
            e.enumsortorder as sort_order
            from pg_type t
            join pg_enum e on t.oid = e.enumtypid
            join pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            where n.nspname = 'public'
            order by enum_schema, enum_name, sort_order`);
    });
});
