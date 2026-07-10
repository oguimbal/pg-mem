import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('IS [NOT] DISTINCT FROM', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('treats NULL as a comparable value', () => {
        expect(one(`select null is distinct from null as v`).v).toEqual(false);
        expect(one(`select null is distinct from 1 as v`).v).toEqual(true);
        expect(one(`select null is not distinct from null as v`).v).toEqual(true);
        expect(one(`select null is not distinct from 1 as v`).v).toEqual(false);
    });

    it('behaves like <> / = for non-null operands', () => {
        expect(one(`select 1 is distinct from 2 as v`).v).toEqual(true);
        expect(one(`select 1 is distinct from 1 as v`).v).toEqual(false);
        expect(one(`select 1 is not distinct from 1 as v`).v).toEqual(true);
    });

    it('filters rows including NULLs', () => {
        db.public.none(`create table t (a int, b int);
                        insert into t values (1, 1), (1, 2), (null, 1), (null, null)`);
        expect(one(`select count(*)::int as c from t where a is distinct from b`).c).toEqual(2);
        expect(one(`select count(*)::int as c from t where a is not distinct from b`).c).toEqual(2);
    });
});
