import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('array_remove / array_replace / row_to_json / array_to_json', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
    });

    it('array_remove removes all matching elements', () => {
        expect(one(`select array_remove(array[1, 2, 1, 3], 1) as r`).r).toEqual([2, 3]);
        expect(one(`select array_remove(array['a', 'b', 'a'], 'a') as r`).r).toEqual(['b']);
    });

    it('array_remove with null removes null elements', () => {
        expect(one(`select array_remove(array[1, null, 2, null], null) as r`).r).toEqual([1, 2]);
    });

    it('array_replace swaps matching elements', () => {
        expect(one(`select array_replace(array[1, 2, 1], 1, 9) as r`).r).toEqual([9, 2, 9]);
    });

    it('row_to_json builds an object from a row', () => {
        expect(one(`select row_to_json(t) as j from (select 1 a, 'x' b) t`).j).toEqual({ a: 1, b: 'x' });
    });

    it('array_to_json builds a json array', () => {
        expect(one(`select array_to_json(array[1, 2, 3]) as j`).j).toEqual([1, 2, 3]);
    });
});
