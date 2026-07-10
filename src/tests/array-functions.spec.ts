import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('Array functions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    describe('set-returning in FROM', () => {
        it('generate_series yields one row per value', () => {
            expect(many(`select * from generate_series(1, 3)`))
                .toEqual([{ generate_series: 1 }, { generate_series: 2 }, { generate_series: 3 }]);
        });

        it('generate_series honors step and alias', () => {
            expect(many(`select * from generate_series(5, 1, -2) as g`))
                .toEqual([{ g: 5 }, { g: 3 }, { g: 1 }]);
            expect(many(`select * from generate_series(1, 2, 5)`))
                .toEqual([{ generate_series: 1 }]);
        });

        it('generate_series rejects zero step', () => {
            expectQueryError(() => many(`select * from generate_series(1, 3, 0)`), /step size cannot equal zero/);
        });

        it('unnest enumerates array elements', () => {
            expect(many(`select * from unnest(array[10, 20]) as u`))
                .toEqual([{ u: 10 }, { u: 20 }]);
        });

        it('set-returning functions can be joined', () => {
            expect(many(`select g, x from generate_series(1, 2) g, (values (10)) v(x)`))
                .toEqual([{ g: 1, x: 10 }, { g: 2, x: 10 }]);
        });
    });

    describe('scalar array functions', () => {
        it('array_length and cardinality', () => {
            expect(one(`select array_length(array[1,2,3], 1) as v`).v).toBe(3);
            expect(one(`select array_length(array[1,2,3], 2) as v`).v).toBeNull();
            expect(one(`select cardinality(array[1,2,3]) as v`).v).toBe(3);
        });

        it('array_upper and array_lower', () => {
            expect(one(`select array_upper(array[7,8], 1) as v`).v).toBe(2);
            expect(one(`select array_lower(array[7,8], 1) as v`).v).toBe(1);
        });

        it('array_append handles nulls like pg', () => {
            expect(one(`select array_append(array[1,2], 3) as v`).v).toEqual([1, 2, 3]);
            expect(one(`select array_append(array[1], null) as v`).v).toEqual([1, null]);
        });

        it('array_cat', () => {
            expect(one(`select array_cat(array[1,2], array[3,4]) as v`).v).toEqual([1, 2, 3, 4]);
        });

        it('array_position is 1-based and null when absent', () => {
            expect(one(`select array_position(array['a','b','c'], 'b') as v`).v).toBe(2);
            expect(one(`select array_position(array['a'], 'z') as v`).v).toBeNull();
        });

        it('array_to_string skips nulls unless given a null text', () => {
            expect(one(`select array_to_string(array[1,2,3], ',') as v`).v).toBe('1,2,3');
            expect(one(`select array_to_string(array[1,null,2], ',', '*') as v`).v).toBe('1,*,2');
        });

        it('string_to_array', () => {
            expect(one(`select string_to_array('a,b,c', ',') as v`).v).toEqual(['a', 'b', 'c']);
            expect(one(`select string_to_array('abc', '') as v`).v).toEqual(['abc']);
            expect(one(`select string_to_array('abc', null) as v`).v).toEqual(['a', 'b', 'c']);
        });

        it('rejects non-array arguments', () => {
            expectQueryError(() => one(`select array_length(42, 1)`), /expects an array/);
        });
    });
});
