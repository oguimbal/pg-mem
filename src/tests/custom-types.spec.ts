import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';
import { DataType, QueryError } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('Custom types', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('can create enum type', () => {
        none(`create type myType as enum ('a', 'b')`);
    });


    it('can rename enum type', () => {
        none(`create type myType as enum ('a', 'b')`);
        none(`ALTER TYPE myType RENAME TO myNewType`);
        expect(many(`select 'b'::myNewType;
            `)).toEqual([{ mynewtype: 'b' }]);
        expectQueryError(() => none(`select 'b'::myType;`)
            , /type "mytype" does not exist/);
    });

    it('can add value to enum type', () => {
        none(`create type myType as enum ('a', 'b')`);
        none(`ALTER TYPE myType ADD VALUE 'c'`);
    });

    it('can cast to enum', () => {
        expect(many(`create type myType as enum ('a', 'b');
                    select 'b'::myType;
            `)).toEqual([{ mytype: 'b' }]);
    });

    it('can convert enum back to text', () => {
        expect(many(`create type myType as enum ('a', 'b');
                    select 'b'::myType::text;
            `)).toEqual([{ text: 'b' }]);
    });

    it('cannot convert enum to something else', () => {
        expectQueryError(() => none(`create type myType as enum ('a', 'b');
                    select 'b'::myType::int;`)
            , /cannot cast type mytype to integer/);
    });

    it('cannot cast invalid enum', () => {
        expectQueryError(() => none(`create type myType as enum ('a', 'b');
                                    select 'c'::myType;`)
            , /invalid input value for enum mytype: "c"/);
    });

    it('can create tables with values it it', () => {
        expect(many(`create type myType as enum ('a', 'b');
                create table test (val mytype);
                insert into test values ('a');
                select * from test`))
            .toEqual([{ val: 'a' }])
    });


    it('cannot insert invalid enum values', () => {
        none(`create type myType as enum ('a', 'b');
                create table test (val mytype);`);

        expectQueryError(() => none(`insert into test values ('c');`)
            , /invalid input value for enum mytype: "c"/);
    });


    it('can register custom type', () => {
        db.public.registerEquivalentType({
            name: 'custom',
            equivalentTo: DataType.text,
            isValid(val: string) {
                if (val === 'throw') {
                    throw new QueryError('Nope');
                }
                return val === 'something';
            }
        });

        none(`SELECT 'something'::custom`);

        expectQueryError(() => none(`SELECT 'throw'::custom`), /Nope/);
        expectQueryError(() => none(`SELECT 'whatever'::custom`), /invalid input syntax for type custom/);
        expectQueryError(() => none(`SELECT 42::custom`), /cannot cast type integer to custom/);
    });

    it('can register custom type with length', () => {
        db.public.registerEquivalentSizableType({
            name: 'vector',
            equivalentTo: DataType.text,
            isValid(val: string) {
                return true;
            }
        });

        none(`CREATE TABLE "test" ("embedding" vector(1536) NOT NULL)`);
    })
});
