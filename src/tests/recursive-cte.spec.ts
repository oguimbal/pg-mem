import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('WITH RECURSIVE', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('counts recursively', () => {
        expect(many(`with recursive c(n) as (
                        select 1
                        union all
                        select n + 1 from c where n < 3
                    ) select n from c`))
            .toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    });

    it('traverses a hierarchy', () => {
        none(`create table org (id int, parent int);
              insert into org values (1, null), (2, 1), (3, 2), (4, 1)`);
        expect(many(`with recursive tree(id, parent, depth) as (
                        select id, parent, 0 from org where parent is null
                        union all
                        select o.id, o.parent, t.depth + 1 from org o join tree t on o.parent = t.id
                    ) select id, depth from tree order by id`))
            .toEqual([
                { id: 1, depth: 0 },
                { id: 2, depth: 1 },
                { id: 3, depth: 2 },
                { id: 4, depth: 1 },
            ]);
    });

    it('builds paths with accumulators', () => {
        none(`create table org (id int, parent int, name text);
              insert into org values (1, null, 'root'), (2, 1, 'a'), (3, 2, 'b')`);
        expect(many(`with recursive tree(id, path) as (
                        select id, name from org where parent is null
                        union all
                        select o.id, t.path || '/' || o.name from org o join tree t on o.parent = t.id
                    ) select path from tree order by id`))
            .toEqual([{ path: 'root' }, { path: 'root/a' }, { path: 'root/a/b' }]);
    });

    it('union (without all) deduplicates and terminates cycles', () => {
        // would loop forever with UNION ALL
        expect(many(`with recursive c(n) as (
                        select 1
                        union
                        select n from c
                    ) select n from c`))
            .toEqual([{ n: 1 }]);
    });

    it('recursive term sees only the previous iteration', () => {
        // each iteration doubles from the last working set only: 1 → 2 → 4 (not 1,2,3,4,...)
        expect(many(`with recursive c(n) as (
                        select 1
                        union all
                        select n * 2 from c where n < 4
                    ) select n from c order by n`))
            .toEqual([{ n: 1 }, { n: 2 }, { n: 4 }]);
    });

    it('rejects mismatched column count', () => {
        expectQueryError(() => many(`with recursive c(a, b) as (
                            select 1
                            union all
                            select n + 1 from c where a < 3
                        ) select a from c`), /1 columns available but 2 columns specified/);
    });

    it('can aggregate over the recursion result', () => {
        expect(many(`with recursive c(n) as (
                        select 1 union all select n + 1 from c where n < 100
                    ) select sum(n) as s, count(*) as cnt from c`))
            .toEqual([{ s: 5050, cnt: 100 }]);
    });
});
