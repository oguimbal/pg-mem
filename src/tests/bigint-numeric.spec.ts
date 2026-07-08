import { describe, it, beforeEach, expect } from 'bun:test';
import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

// verified against postgres 16 (numeric/bigint return as strings, like node-postgres)
describe('bigint & numeric precision', () => {
    let db: IMemoryDb;
    let one: (s: string) => any;
    beforeEach(() => { db = newDb(); one = db.public.one.bind(db.public); });

    describe('bigint', () => {
        it('keeps 64-bit precision (beyond JS safe integers)', () => {
            expect(one(`select (9007199254740993::bigint)::text as r`).r).toBe('9007199254740993');
            expect(one(`select 9223372036854775807::bigint as r`).r).toBe('9223372036854775807');
        });
        it('arithmetic stays exact', () => {
            expect(one(`select (9007199254740992::bigint + 1)::text as r`).r).toBe('9007199254740993');
            expect(one(`select (1000000000000::bigint * 1000000)::text as r`).r).toBe('1000000000000000000');
        });
        it('a large integer literal is bigint', () => {
            expect(one(`select 9007199254740993 as r`).r).toBe('9007199254740993');
        });
    });

    describe('numeric', () => {
        it('rounds to scale, half away from zero', () => {
            expect(one(`select (1.005::numeric(10,2))::text as r`).r).toBe('1.01');
            expect(one(`select (2.5::numeric(10,0))::text as r`).r).toBe('3');
        });
        it('division keeps 20 fractional digits', () => {
            expect(one(`select (1::numeric / 3::numeric)::text as r`).r).toBe('0.33333333333333333333');
        });
        it('is returned as a string', () => {
            expect(one(`select 1.5::numeric as r`).r).toBe('1.5');
        });
    });

    describe('integer overflow', () => {
        it('int4 arithmetic overflow errors', () => {
            expectQueryError(() => one(`select 2147483647::int + 1`), /integer out of range/);
            expectQueryError(() => one(`select 2000000000::int * 2`), /integer out of range/);
        });
        it('bigint does not overflow at int4 range', () => {
            expect(one(`select (2147483647::bigint + 1)::text as r`).r).toBe('2147483648');
        });
    });

    it('regular integer and float behaviour is unchanged (numbers)', () => {
        expect(one(`select 2 + 3 as r`).r).toBe(5);
        expect(one(`select 1.5 + 2.5 as r`).r).toBe(4);
        expect(one(`select 7 / 2 as r`).r).toBe(3);
    });
});
