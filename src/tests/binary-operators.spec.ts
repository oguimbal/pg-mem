import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { IMemoryDb } from '../interfaces-private';
import { expectQueryError } from './test-utils';

describe('Binary operators', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let one: (str: string) => any;
    function all(table = 'test') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });



    it('[bugfix] can substract date & ints', () => {
        // was throwing (cannot cast type integer to date)
        //  => https://github.com/oguimbal/pg-mem/issues/172
        expect(one(`select CURRENT_DATE - 7 as dt`).dt).toBeInstanceOf(Date);
        expect(one(`select CURRENT_DATE + 7 as dt`).dt).toBeInstanceOf(Date);
        expect(one(`select 7 + CURRENT_DATE as dt`).dt).toBeInstanceOf(Date);

        // only works with ints
        expectQueryError(() => many(`select 7 - CURRENT_DATE`), /operator does not exist: integer - date/);
    })

    it.skip('should not be able to substract float to date', () => {
        // these should throw (#todo)
        expectQueryError(() => many(`select CURRENT_DATE - 7.5`), /operator does not exist: date - float/);
        expectQueryError(() => many(`select CURRENT_DATE - 7.5::float`), /operator does not exist: date - float/);
    })


    it('supports remove key on json', () => {
        // bugfix of https://github.com/oguimbal/pg-mem/issues/77
        expect(one(`select '["a", "b", "b", "c"]'::jsonb - 'b' as test`))
            .toEqual({ test: ['a', 'c'] });
        expect(one(`select '{"a": "a", "b":"b"}'::jsonb - 'b' as test`))
            .toEqual({ test: { 'a': 'a' } });
    });

    it('supports remove index', () => {
        expect(one(`select '["a", "b", "b", "c"]'::jsonb - 0 as test`))
            .toEqual({ test: ['b', 'b', 'c'] });
        expect(one(`select '["a", "b", "b", "c"]'::jsonb - 3 as test`))
            .toEqual({ test: ['a', 'b', 'b'] });
    })

});
