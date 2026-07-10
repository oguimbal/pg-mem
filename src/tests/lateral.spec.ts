import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('Lateral function calls in FROM', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        none(`create table t (id int, arr int[]);
              insert into t values (1, array[10, 20]), (2, array[30]), (3, null)`);
    });

    it('explicit lateral unnest', () => {
        expect(many(`select id, x from t, lateral unnest(arr) as x order by id, x`))
            .toEqual([
                { id: 1, x: 10 },
                { id: 1, x: 20 },
                { id: 2, x: 30 },
            ]);
    });

    it('implicit lateral (no keyword), filterable', () => {
        expect(many(`select id, x from t, unnest(arr) as x where x > 15 order by id, x`))
            .toEqual([{ id: 1, x: 20 }, { id: 2, x: 30 }]);
    });

    it('correlated generate_series', () => {
        expect(many(`select id, g from t, lateral generate_series(1, id) as g order by id, g`))
            .toEqual([
                { id: 1, g: 1 },
                { id: 2, g: 1 },
                { id: 2, g: 2 },
                { id: 3, g: 1 },
                { id: 3, g: 2 },
                { id: 3, g: 3 },
            ]);
    });

    it('null sets drop the source row', () => {
        expect(many(`select id from t, lateral unnest(arr) as x where id = 3`))
            .toEqual([]);
    });

    it('can aggregate over lateral results', () => {
        expect(many(`select id, count(*) as cnt from t, lateral unnest(arr) as x group by id order by id`))
            .toEqual([{ id: 1, cnt: 2 }, { id: 2, cnt: 1 }]);
    });

    it('independent function calls in FROM are not lateral', () => {
        expect(many(`select id, g from t, generate_series(1, 2) as g where id = 1 order by g`))
            .toEqual([{ id: 1, g: 1 }, { id: 1, g: 2 }]);
    });
});
