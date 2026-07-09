import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('range types', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('constructs and renders int4range canonically', () => {
        expect(one(`select int4range(1, 10) as r`).r).toEqual('[1,10)');
        expect(one(`select int4range(1, 10, '[]') as r`).r).toEqual('[1,11)');
        expect(one(`select '(1,10]'::int4range as r`).r).toEqual('[2,11)');
    });

    it('collapses to empty', () => {
        expect(one(`select int4range(5, 5) as r`).r).toEqual('empty');
        expect(one(`select isempty(int4range(5, 5)) as e`).e).toEqual(true);
        expect(one(`select isempty(int4range(1, 5)) as e`).e).toEqual(false);
    });

    it('contains elements (@>, <@)', () => {
        expect(one(`select int4range(1, 10) @> 5 as c`).c).toEqual(true);
        expect(one(`select int4range(1, 10) @> 10 as c`).c).toEqual(false);
        expect(one(`select 5 <@ int4range(1, 10) as c`).c).toEqual(true);
    });

    it('contains ranges and detects overlap', () => {
        expect(one(`select int4range(1, 10) @> int4range(2, 5) as c`).c).toEqual(true);
        expect(one(`select int4range(2, 5) <@ int4range(1, 10) as c`).c).toEqual(true);
        expect(one(`select int4range(1, 10) && int4range(5, 20) as c`).c).toEqual(true);
        expect(one(`select int4range(1, 5) && int4range(10, 20) as c`).c).toEqual(false);
    });

    it('exposes lower/upper/inc accessors', () => {
        expect(one(`select lower(int4range(3, 10)) as l, upper(int4range(3, 10)) as u`))
            .toEqual({ l: 3, u: 10 });
        expect(one(`select lower_inc(int4range(3, 10)) as li, upper_inc(int4range(3, 10)) as ui`))
            .toEqual({ li: true, ui: false });
    });

    it('supports numeric and date ranges', () => {
        expect(one(`select numrange(1.5, 3.0) @> 2.0 as c`).c).toEqual(true);
        expect(one(`select '[2020-01-01,2020-02-01)'::daterange @> '2020-01-15'::date as c`).c).toEqual(true);
        expect(one(`select '[2020-01-01,2020-02-01)'::daterange @> '2020-03-01'::date as c`).c).toEqual(false);
    });

    it('stores a range column', () => {
        none(`create table t (id int, r int4range);
               insert into t values (1, int4range(1, 10)), (2, '[5,20)'::int4range)`);
        expect(many(`select id, r from t where r @> 7 order by id`))
            .toEqual([{ id: 1, r: '[1,10)' }, { id: 2, r: '[5,20)' }]);
        expect(many(`select id from t where r @> 2 order by id`))
            .toEqual([{ id: 1 }]);
    });
});
