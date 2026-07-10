import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('ordered-set aggregates (WITHIN GROUP)', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('percentile_cont interpolates', () => {
        expect(one(`select percentile_cont(0.5) within group (order by x) as p
                    from (values (1), (2), (3), (4)) v(x)`).p).toEqual(2.5);
        expect(one(`select percentile_cont(0.25) within group (order by x) as p
                    from (values (1), (2), (3), (4), (5)) v(x)`).p).toEqual(2);
    });

    it('percentile_disc picks an existing value', () => {
        expect(one(`select percentile_disc(0.5) within group (order by x) as p
                    from (values (1), (2), (3), (4)) v(x)`).p).toEqual(2);
    });

    it('mode returns the most frequent value', () => {
        expect(one(`select mode() within group (order by x) as m
                    from (values (1), (1), (2), (3)) v(x)`).m).toEqual(1);
    });

    it('works with GROUP BY', () => {
        db.public.none(`create table t (x int, g text);
                        insert into t values (1,'a'),(2,'a'),(3,'a'),(4,'a'),(10,'b'),(10,'b'),(20,'b')`);
        expect(many(`select g, percentile_cont(0.5) within group (order by x) as p
                     from t group by g order by g`))
            .toEqual([{ g: 'a', p: 2.5 }, { g: 'b', p: 10 }]);
    });
});
