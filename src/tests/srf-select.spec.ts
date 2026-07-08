import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('Set-returning functions in select list', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('expands unnest', () => {
        expect(many(`select unnest(array[1, 2])`))
            .toEqual([{ unnest: 1 }, { unnest: 2 }]);
    });

    it('expands generate_series with alias', () => {
        expect(many(`select generate_series(1, 3) as g`))
            .toEqual([{ g: 1 }, { g: 2 }, { g: 3 }]);
    });

    it('expands jsonb SRFs', () => {
        expect(many(`select jsonb_object_keys('{"a":1,"b":2}'::jsonb) as k`))
            .toEqual([{ k: 'a' }, { k: 'b' }]);
    });

    it('repeats scalar columns for each element', () => {
        none(`create table t (id int, tags text[]);
              insert into t values (1, array['x','y']), (2, array['z'])`);
        expect(many(`select id, unnest(tags) as tag from t`))
            .toEqual([
                { id: 1, tag: 'x' },
                { id: 1, tag: 'y' },
                { id: 2, tag: 'z' },
            ]);
    });

    it('drops rows when the set is empty', () => {
        expect(many(`select id, unnest(array[]::int[]) as v from (values (1)) x(id)`))
            .toEqual([]);
        expect(many(`select unnest(null::int[]) as v`))
            .toEqual([]);
    });

    it('advances multiple SRFs in lockstep, padding with nulls', () => {
        expect(many(`select unnest(array[1, 2, 3]) as a, unnest(array['x']) as b`))
            .toEqual([
                { a: 1, b: 'x' },
                { a: 2, b: null },
                { a: 3, b: null },
            ]);
    });

    it('does not expand array-returning non-SRF functions', () => {
        expect(many(`select array_append(array[1], 2) as v`))
            .toEqual([{ v: [1, 2] }]);
        expect(many(`select string_to_array('a,b', ',') as v`))
            .toEqual([{ v: ['a', 'b'] }]);
    });

    it('does not expand a plain array column referenced alongside an SRF', () => {
        none(`create table t (arr int[]);
              insert into t values (array[1, 2])`);
        expect(many(`select arr, unnest(arr) as v from t`))
            .toEqual([
                { arr: [1, 2], v: 1 },
                { arr: [1, 2], v: 2 },
            ]);
    });

    it('limit applies after expansion', () => {
        expect(many(`select generate_series(1, 10) as g limit 3`))
            .toEqual([{ g: 1 }, { g: 2 }, { g: 3 }]);
    });
});
