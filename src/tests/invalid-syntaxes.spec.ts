import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

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
        expectQueryError(() => none(`create table test(val integer);
                create index on test(val);
                insert into test values (1), (2), (3), (4)
                select * from test where val >= 2;`)); //   ^  missing a ";" ... but was not throwing.
    })
});