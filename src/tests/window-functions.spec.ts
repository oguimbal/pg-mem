import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('Window functions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
    });

    it('row_number over order by', () => {
        expect(many(`select x, row_number() over (order by x) as n from (values (20), (10)) v(x) order by x`))
            .toEqual([{ x: 10, n: 1 }, { x: 20, n: 2 }]);
    });

    it('row_number does not change output order', () => {
        expect(many(`select x, row_number() over (order by x desc) as n from (values (1), (3), (2)) v(x)`))
            .toEqual([{ x: 1, n: 3 }, { x: 3, n: 1 }, { x: 2, n: 2 }]);
    });

    it('rank and dense_rank handle ties', () => {
        expect(many(`select v, rank() over (order by v) as r, dense_rank() over (order by v) as d
                    from (values (1), (1), (2)) t(v)`))
            .toEqual([
                { v: 1, r: 1, d: 1 },
                { v: 1, r: 1, d: 1 },
                { v: 2, r: 3, d: 2 },
            ]);
    });

    it('partitions windows independently', () => {
        expect(many(`select grp, v, rank() over (partition by grp order by v) as r
                    from (values ('a', 10), ('b', 5), ('a', 20)) t(grp, v)`))
            .toEqual([
                { grp: 'a', v: 10, r: 1 },
                { grp: 'b', v: 5, r: 1 },
                { grp: 'a', v: 20, r: 2 },
            ]);
    });

    it('aggregates over the whole partition without order by', () => {
        expect(many(`select x, sum(x) over () as s, avg(x) over () as a, count(*) over () as c
                    from (values (1), (2), (3)) v(x)`))
            .toEqual([
                { x: 1, s: 6, a: 2, c: 3 },
                { x: 2, s: 6, a: 2, c: 3 },
                { x: 3, s: 6, a: 2, c: 3 },
            ]);
    });

    it('computes running aggregates with order by', () => {
        expect(many(`select x, sum(x) over (order by x) as s from (values (3), (1), (2)) v(x)`))
            .toEqual([
                { x: 3, s: 6 },
                { x: 1, s: 1 },
                { x: 2, s: 3 },
            ]);
    });

    it('running aggregates include the full peer group', () => {
        expect(many(`select v, sum(v) over (order by v) as s from (values (1), (1), (2)) t(v)`))
            .toEqual([
                { v: 1, s: 2 },
                { v: 1, s: 2 },
                { v: 2, s: 4 },
            ]);
    });

    it('min/max over window', () => {
        expect(many(`select x, min(x) over () as mn, max(x) over () as mx from (values (5), (2), (9)) v(x)`))
            .toEqual([
                { x: 5, mn: 2, mx: 9 },
                { x: 2, mn: 2, mx: 9 },
                { x: 9, mn: 2, mx: 9 },
            ]);
    });

    it('lag and lead with defaults', () => {
        expect(many(`select x, lag(x) over (order by x) as prev, lead(x, 1, -1) over (order by x) as nxt
                    from (values (1), (2), (3)) v(x)`))
            .toEqual([
                { x: 1, prev: null, nxt: 2 },
                { x: 2, prev: 1, nxt: 3 },
                { x: 3, prev: 2, nxt: -1 },
            ]);
    });

    it('ntile distributes extra rows to the first buckets', () => {
        expect(many(`select x, ntile(2) over (order by x) as b from (values (1), (2), (3)) v(x)`))
            .toEqual([
                { x: 1, b: 1 },
                { x: 2, b: 1 },
                { x: 3, b: 2 },
            ]);
    });

    it('first_value and last_value follow the default frame', () => {
        expect(many(`select x, first_value(x) over (order by x) as f, last_value(x) over (order by x) as l
                    from (values (1), (2), (3)) v(x)`))
            .toEqual([
                { x: 1, f: 1, l: 1 },
                { x: 2, f: 1, l: 2 },
                { x: 3, f: 1, l: 3 },
            ]);
    });

    it('windowed aggregate is not an implicit group aggregation', () => {
        // must yield one row per source row, not collapse to one
        expect(many(`select sum(x) over () as s from (values (1), (2)) v(x)`).length).toBe(2);
    });

    it('works on real tables with where clauses', () => {
        many(`create table sales (region text, amount int);
              insert into sales values ('n', 10), ('n', 20), ('s', 5), ('s', 30), ('s', 1)`);
        expect(many(`select region, amount, rank() over (partition by region order by amount desc) as r
                    from sales where amount > 1`))
            .toEqual([
                { region: 'n', amount: 10, r: 2 },
                { region: 'n', amount: 20, r: 1 },
                { region: 's', amount: 5, r: 2 },
                { region: 's', amount: 30, r: 1 },
            ]);
    });

    it('rejects window functions in where', () => {
        expect(() => many(`select x from (values (1)) v(x) where row_number() over () = 1`))
            .toThrow(/window functions are not allowed/);
    });

    it('two windows in one query', () => {
        expect(many(`select x, row_number() over (order by x) as asc_n, row_number() over (order by x desc) as desc_n
                    from (values (1), (2)) v(x)`))
            .toEqual([
                { x: 1, asc_n: 1, desc_n: 2 },
                { x: 2, asc_n: 2, desc_n: 1 },
            ]);
    });
});
