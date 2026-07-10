import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('numeric functions and bitwise/exponent operators', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
    });

    it('div / gcd / lcm / factorial', () => {
        expect(one(`select div(7, 2) as v`).v).toEqual(3);
        expect(one(`select gcd(12, 8) as v`).v).toEqual(4);
        expect(one(`select lcm(4, 6) as v`).v).toEqual(12);
        expect(one(`select factorial(5) as v`).v).toEqual(120);
    });

    it('width_bucket and bit_length', () => {
        expect(one(`select width_bucket(5, 1, 10, 3) as v`).v).toEqual(2);
        expect(one(`select bit_length('abc') as v`).v).toEqual(24);
    });

    it('div by zero errors', () => {
        expectQueryError(() => one(`select div(1, 0)`), /division by zero/);
    });

    it('bitwise operators', () => {
        expect(one(`select 5 & 3 as v`).v).toEqual(1);
        expect(one(`select 5 | 2 as v`).v).toEqual(7);
        expect(one(`select 5 # 1 as v`).v).toEqual(4);
        expect(one(`select 5 << 2 as v`).v).toEqual(20);
        expect(one(`select 20 >> 2 as v`).v).toEqual(5);
    });

    it('exponentiation operator', () => {
        expect(one(`select (2 ^ 10)::int as v`).v).toEqual(1024);
    });
});
