import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('interval week/decade/century/millennium units', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
    });

    it('normalizes weeks to days', () => {
        expect(one(`select interval '1 week' as i`).i).toEqual({ days: 7 });
        expect(one(`select interval '2 weeks 3 days' as i`).i).toEqual({ days: 17 });
    });

    it('normalizes decade/century/millennium to years', () => {
        expect(one(`select interval '1 decade' as i`).i).toEqual({ years: 10 });
        expect(one(`select interval '1 century' as i`).i).toEqual({ years: 100 });
        expect(one(`select interval '2 millenniums' as i`).i).toEqual({ years: 2000 });
    });

    it('adds a week interval to a date', () => {
        expect(one(`select to_char(date '2020-01-01' + interval '2 weeks', 'YYYY-MM-DD') as d`).d)
            .toEqual('2020-01-15');
    });
});

describe('window functions nth_value / cume_dist / percent_rank', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
    });

    it('nth_value over the default frame', () => {
        expect(many(`select x, nth_value(x, 2) over (order by x) as v
                     from (values (10), (20), (30)) t(x) order by x`))
            .toEqual([{ x: 10, v: null }, { x: 20, v: 20 }, { x: 30, v: 20 }]);
    });

    it('cume_dist', () => {
        expect(many(`select x, cume_dist() over (order by x) as c
                     from (values (1), (2), (2), (3)) t(x) order by x`))
            .toEqual([{ x: 1, c: 0.25 }, { x: 2, c: 0.75 }, { x: 2, c: 0.75 }, { x: 3, c: 1 }]);
    });

    it('percent_rank', () => {
        expect(many(`select x, percent_rank() over (order by x) as p
                     from (values (1), (3), (3)) t(x) order by x`))
            .toEqual([{ x: 1, p: 0 }, { x: 3, p: 0.5 }, { x: 3, p: 0.5 }]);
    });
});
