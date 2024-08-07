import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

// https://www.postgresql.org/docs/current/typeconv-union-case.html

describe('Union', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('returns union', () => {
        expect(many(`select a as int, b||' answer' as str from (values (1,'one')) as fst(a,b) union (select * from (values (2,'two')) as snd(c,d))`))
            .toEqual([
                { int: 1, str: 'one answer' },
                { int: 2, str: 'two' },
            ])
    });

    it('works when begining with an enclosed statemnt', () => {
        expect(many(`(SELECT 'a' as str UNION SELECT NULL) UNION SELECT 'b';`))
            .toEqual([
                { str: 'a' },
                { str: null },
                { str: 'b' },
            ]);
    });

    it('fails when non matching items count', () => {
        expectQueryError(() => many(`select * from (values ('a')) as ta union select * from (values ('a', 4)) as tb`)
            , /each UNION query must have the same number of columns/);
    });

    it('cannot cast', () => {
        expectQueryError(() => many(`select * from (values ('1')) as ta union select * from (values (2)) as tb`)
            , /UNION types text.* and integer.* cannot be matched/);
    });

    it('respects simple union', () => {
        expect(many(`select 1 v union select 1 x`))
            .toEqual([{ v: 1 }]);
        expect(many(`select null v union select null x`))
            .toEqual([{ v: null }]);
    });

    it('respects union all', () => {
        expect(many(`select 1 v union all select 1 x`))
            .toEqual([
                { v: 1 },
                { v: 1 },
            ]);
        expect(many(`select null v union all select null x`))
            .toEqual([
                { v: null },
                { v: null },
            ]);
    });
});
