import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('Limits', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });


    it('simple limit', () => {
        expect(many(`create table test(val text);
            insert into test values ('a'), ('b'), ('c');
            select val from test limit 2`))
            .toEqual([
                { val: 'a' }
                , { val: 'b' }
            ]);
    });

    it('offset + limit', () => {
        expect(many(`create table test(val text);
            insert into test values ('a'), ('b'), ('c');
            select val from test limit 1 offset 1`))
            .toEqual([
                { val: 'b' }
            ]);
    });
});