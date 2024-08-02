import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { _ITable } from '../interfaces-private';
import { expectQueryError } from './test-utils';

describe('regclass', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('can select pg_catalog tables as regclass', () => {
        expect(many(`select 'pg_class'::regclass`))
            .toEqual([{ regclass: 'pg_class' }])
    })

    it('can select local table as regclass', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regclass`))
            .toEqual([{
                regclass: 'test'
            }])
    });

    it('fails on non existing type', () => {
        expectQueryError(() => none(`select 'xxx'::regclass;`), /relation "xxx" does not exist/);
        expectQueryError(() => none(`select 'text'::regclass;`), /relation "text" does not exist/);
        expectQueryError(() => many(`select '25abc'::regclass`), /relation "25abc" does not exist/)
    });


    it('can cast back to string', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regclass::text`))
            .toEqual([{
                text: 'test'
            }])
    });


    it('can cast back table to integer', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regclass::integer`))
            .toEqual([{
                integer: (db.public.getTable('test') as _ITable).reg.classId
            }])
    });


    it('can cast existing from int', () => {
        none(`create table test (a text);`);
        const rt = (db.public.getTable('test') as _ITable).reg.classId;
        expect(rt).toBeNumber();
        expect(many(`select ${rt}::regclass as asint, '${rt}'::regclass as asstr`))
            .toEqual([{
                asint: rt,
                asstr: 'test',
            }])
    });

    it('can cast non existing from int', () => {
        expect(many(`select 42424242::regclass`))
            .toEqual([{
                regclass: 42424242,
            }]);;
    })
});