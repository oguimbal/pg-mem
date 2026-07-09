import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('set operations (INTERSECT / EXCEPT)', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        db.public.none(`create table a (x int); create table b (x int);
                        insert into a values (1), (2), (2), (3);
                        insert into b values (2), (2), (3), (4)`);
    });

    it('INTERSECT returns distinct common rows', () => {
        expect(many(`select x from a intersect select x from b order by x`))
            .toEqual([{ x: 2 }, { x: 3 }]);
    });

    it('EXCEPT returns distinct left-only rows', () => {
        expect(many(`select x from a except select x from b order by x`))
            .toEqual([{ x: 1 }]);
    });

    it('INTERSECT ALL keeps min multiplicity', () => {
        expect(many(`select x from a intersect all select x from b order by x`))
            .toEqual([{ x: 2 }, { x: 2 }, { x: 3 }]);
    });

    it('EXCEPT ALL subtracts multiplicity', () => {
        expect(many(`select x from a except all select x from b order by x`))
            .toEqual([{ x: 1 }]);
    });

    it('treats NULLs as equal', () => {
        db.public.none(`create table c (x int); create table d (x int);
                        insert into c values (1), (null); insert into d values (null)`);
        expect(many(`select x from c intersect select x from d`)).toEqual([{ x: null }]);
    });

    it('chains with union and parentheses', () => {
        expect(many(`(select x from a intersect select x from b) except select 3 order by 1`))
            .toEqual([{ x: 2 }]);
    });
});
