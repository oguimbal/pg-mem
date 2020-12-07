import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';

describe('Constraints', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('cannot add check constraint when value not matching', () => {
        none(`create table test(val int);
                insert into test values (42);`);
        assert.throws(() => none(`alter table test add constraint cstname check (val < 10)`)
            , /check constraint "cstname" is violated by some row/);
    })


    it('can add check constraint when value matching', () => {
        none(`create table test(val int);
                insert into test values (42);`);
        none(`alter table test add constraint cstname check (val < 100)`);
    });


    it ('accepts collations', () => {
        none(`CREATE TABLE public.crafter
        (
            crafter_name_first character varying COLLATE pg_catalog."default" NOT NULL,
            crafter_name_last character varying COLLATE pg_catalog."default" NOT NULL
        )`);
    })
});