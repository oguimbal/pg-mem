import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { DataType, QueryError } from '../interfaces';

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
            `)).to.deep.equal([{ mynewtype: 'b' }]);
        assert.throws(() => none(`select 'b'::myType;`)
            , /type "mytype" does not exist/);
    });

    it('can add value to enum type', () => {
        none(`create type myType as enum ('a', 'b')`);
        none(`ALTER TYPE myType ADD VALUE 'c'`);
    });

    it('can cast to enum', () => {
        expect(many(`create type myType as enum ('a', 'b');
                    select 'b'::myType;
            `)).to.deep.equal([{ mytype: 'b' }]);
    });

    it('can convert enum back to text', () => {
        expect(many(`create type myType as enum ('a', 'b');
                    select 'b'::myType::text;
            `)).to.deep.equal([{ text: 'b' }]);
    });

    it('cannot convert enum to something else', () => {
        assert.throws(() => none(`create type myType as enum ('a', 'b');
                    select 'b'::myType::int;`)
            , /cannot cast type mytype to integer/);
    });

    it('cannot cast invalid enum', () => {
        assert.throws(() => none(`create type myType as enum ('a', 'b');
                                    select 'c'::myType;`)
            , /invalid input value for enum mytype: "c"/);
    });

    it('can create tables with values it it', () => {
        expect(many(`create type myType as enum ('a', 'b');
                create table test (val mytype);
                insert into test values ('a');
                select * from test`))
            .to.deep.equal([{ val: 'a' }])
    });


    it('cannot insert invalid enum values', () => {
        none(`create type myType as enum ('a', 'b');
                create table test (val mytype);`);

        assert.throws(() => none(`insert into test values ('c');`)
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

        assert.throws(() => none(`SELECT 'throw'::custom`), /Nope/);
        assert.throws(() => none(`SELECT 'whatever'::custom`), /invalid input syntax for type custom/);
        assert.throws(() => none(`SELECT 42::custom`), /cannot cast type integer to custom/);
    })
});
