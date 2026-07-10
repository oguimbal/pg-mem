import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('ALL(array) quantifier', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('evaluates <> ALL and = ALL', () => {
        expect(one(`select 1 <> all(array[2, 3]) as v`).v).toEqual(true);
        expect(one(`select 1 = all(array[1, 1]) as v`).v).toEqual(true);
        expect(one(`select 1 = all(array[1, 2]) as v`).v).toEqual(false);
    });

    it('evaluates comparison quantifiers', () => {
        expect(one(`select 5 > all(array[1, 2, 3]) as v`).v).toEqual(true);
        expect(one(`select 5 > all(array[1, 9]) as v`).v).toEqual(false);
    });

    it('filters a table with > ALL', () => {
        db.public.none(`create table t (x int); insert into t values (1), (5), (10)`);
        expect(many(`select x from t where x > all(array[2, 3]) order by x`))
            .toEqual([{ x: 5 }, { x: 10 }]);
    });

    it('still supports ANY', () => {
        expect(one(`select 1 = any(array[1, 2]) as v`).v).toEqual(true);
        expect(one(`select 9 = any(array[1, 2]) as v`).v).toEqual(false);
    });
});

describe('generate_series over timestamps', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
    });

    it('steps by day', () => {
        expect(many(`select to_char(g, 'YYYY-MM-DD') as d
                     from generate_series(timestamp '2020-01-01', timestamp '2020-01-03', interval '1 day') g`))
            .toEqual([{ d: '2020-01-01' }, { d: '2020-01-02' }, { d: '2020-01-03' }]);
    });

    it('steps by month', () => {
        expect(many(`select to_char(g, 'YYYY-MM-DD') as d
                     from generate_series(timestamp '2020-01-01', timestamp '2020-03-01', interval '1 month') g`))
            .toEqual([{ d: '2020-01-01' }, { d: '2020-02-01' }, { d: '2020-03-01' }]);
    });

    it('steps backwards with a negative interval', () => {
        expect(many(`select to_char(g, 'YYYY-MM-DD') as d
                     from generate_series(timestamp '2020-01-03', timestamp '2020-01-01', interval '-1 day') g`))
            .toEqual([{ d: '2020-01-03' }, { d: '2020-01-02' }, { d: '2020-01-01' }]);
    });
});
