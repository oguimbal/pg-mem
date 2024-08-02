import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('Naming & collisions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('prevents "table <-> table" collisions', () => {
        none('create table test(a text);');
        expectQueryError(() => none('create table test(a text);'), /relation "test" already exists/);
    });

    it('prevents "table <-> index" collisions', () => {
        none('create table test(a text);');
        expectQueryError(() => none('create index test on test(a)'), /relation "test" already exists/);
    });

    it('prevents "index <-> table" collisions', () => {
        none(`create table other(a text);
                create index test on other(a);`);
        expectQueryError(() => none('create table test(a text);'), /relation "test" already exists/);
    });

    it('prevents "enum <-> table" collisions', () => {
        none(`create table test(a text);`);
        expectQueryError(() => none(`create type test as enum ('a');`), /type "test" already exists/);
    });

    it('prevents "table <-> sequence" collisions', () => {
        none('create table test(a text);');
        expectQueryError(() => none('create sequence test'), /relation "test" already exists/);
    });

    it('prevents "sequence <-> table" collisions', () => {
        none('create sequence test');
        expectQueryError(() => none('create table test(a text);'), /relation "test" already exists/);
    });

    it('prevents "index <-> sequence" collisions', () => {
        none(`create table other(a text);
            create index test on other(a);`);
        expectQueryError(() => none('create sequence test'), /relation "test" already exists/);
    });

    it('prevents "sequence <-> index" collisions', () => {
        none(`create table other(a text);
                create sequence test;`);
        expectQueryError(() => none('create index test on other(a);'), /relation "test" already exists/);
    });

    it('prevents droping an unqualified table when overriden by system one', () => {
        none(`create table pg_class(a text);`);
        expectQueryError(() => none(`drop table pg_class`), /permission denied: "pg_class" is a system catalog/);
        // allows it when fully qualified
        none(`drop table public.pg_class;`);
    });


    it('overrides local tables by system table', () => {
        expect(many(`create table pg_class (a text);
            insert into public.pg_class values ('a');
            select * from pg_class`))
            .toEqual([]);

        // allows it when fully qualified
        expect(many(`select * from public.pg_class`)).toEqual([{ a: 'a' }]);
    });


    it('accepts alter tables with cased names', () => {
        none(`create table "TeSt" (a text);
                alter table "TeSt" add b text;`)
    })

    it('accepts " in names', () => {
        expect(many(`create table "a""b"(a text);
                    insert into "a""b" values ('42');
                    select * from "a""b";`))
            .toEqual([{ a: '42' }]);
    });


    it('selects lowercased version when not quoted', () => {
        expect(many(`create table test(mycol text);
                insert into test values ('test');
                select MYCOL from test;`))
            .toEqual([{ mycol: 'test' }])
    });
});