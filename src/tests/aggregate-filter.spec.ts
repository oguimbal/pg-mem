import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('aggregate FILTER', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
        db.public.none(`create table t (x int, g text);
                        insert into t values (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b')`);
    });

    it('filters count(*)', () => {
        expect(one(`select count(*) filter (where x > 1) as c from t`).c).toEqual(4 - 1);
    });

    it('filters sum', () => {
        expect(one(`select sum(x) filter (where x > 2) as s from t`).s).toEqual(7);
    });

    it('applies per-aggregate filters independently', () => {
        expect(one(`select count(*) as c, count(*) filter (where x > 2) as f from t`))
            .toEqual({ c: 4, f: 2 });
    });

    it('works with GROUP BY', () => {
        expect(many(`select g, count(*) filter (where x > 1) as c from t group by g order by g`))
            .toEqual([{ g: 'a', c: 1 }, { g: 'b', c: 2 }]);
    });

    it('returns 0 when the filter matches nothing', () => {
        expect(one(`select count(*) filter (where x > 10) as c from t`).c).toEqual(0);
    });

    it('combines with DISTINCT', () => {
        db.public.none(`insert into t values (2, 'a'), (3, 'b')`);
        expect(one(`select count(distinct x) filter (where x > 1) as c from t`).c).toEqual(3);
    });
});
