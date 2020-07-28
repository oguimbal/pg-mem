import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';

describe('Invalid syntaxes', () => {

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



    it('checks this is an invalid syntax', () => {
        assert.throws(() => none(`create table test(val integer);
                create index on test(val);
                insert into test values (1), (2), (3), (4)
                select * from test where val >= 2;`)); //   ^  missing a ";" ... but was not throwing.
    })
});