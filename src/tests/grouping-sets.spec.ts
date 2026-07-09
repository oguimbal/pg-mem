import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('GROUP BY ROLLUP / CUBE', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        db.public.none(`create table s (g text, sub text, amt int);
                        insert into s values ('a','x',1), ('a','y',2), ('b','x',3)`);
    });

    it('ROLLUP produces subtotals and a grand total', () => {
        expect(many(`select g, sub, sum(amt)::int as s from s
                     group by rollup(g, sub)
                     order by g nulls last, sub nulls last`))
            .toEqual([
                { g: 'a', sub: 'x', s: 1 },
                { g: 'a', sub: 'y', s: 2 },
                { g: 'a', sub: null, s: 3 },
                { g: 'b', sub: 'x', s: 3 },
                { g: 'b', sub: null, s: 3 },
                { g: null, sub: null, s: 6 },
            ]);
    });

    it('CUBE produces all combinations', () => {
        expect(many(`select g, sub, sum(amt)::int as s from s
                     group by cube(g, sub)
                     order by g nulls last, sub nulls last`))
            .toEqual([
                { g: 'a', sub: 'x', s: 1 },
                { g: 'a', sub: 'y', s: 2 },
                { g: 'a', sub: null, s: 3 },
                { g: 'b', sub: 'x', s: 3 },
                { g: 'b', sub: null, s: 3 },
                { g: null, sub: 'x', s: 4 },
                { g: null, sub: 'y', s: 2 },
                { g: null, sub: null, s: 6 },
            ]);
    });

    it('mixes plain columns with ROLLUP', () => {
        expect(many(`select g, sub, sum(amt)::int as s from s
                     group by g, rollup(sub)
                     order by g, sub nulls last`))
            .toEqual([
                { g: 'a', sub: 'x', s: 1 },
                { g: 'a', sub: 'y', s: 2 },
                { g: 'a', sub: null, s: 3 },
                { g: 'b', sub: 'x', s: 3 },
                { g: 'b', sub: null, s: 3 },
            ]);
    });
});
