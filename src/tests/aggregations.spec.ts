import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';

describe('Aggregations', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;

    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('supports max()', () => {
        expect(many(`create table example(a int);
                    insert into example values (3), (null), (1), (5), (2);
                    select max(a) from example`))
            .toEqual([{ max: 5 }]);
    });

    it('max() returns null when nothing', () => {
        expect(many(`create table example(a int);
                    select max(a) from example`))
            .toEqual([{ max: null }]);
    });


    it('supports min()', () => {
        expect(many(`create table example(a int);
                    insert into example values (3), (null), (1), (5), (2);
                    select min(a) from example`))
            .toEqual([{ min: 1 }]);
    });

    it('min() returns null when nothing', () => {
        expect(many(`create table example(a int);
                    select min(a) from example`))
            .toEqual([{ min: null }]);
    });

    it('supports sum()', () => {
        expect(many(`create table example(a int);
                  insert into example values (1), (-2), (null), (3), (-4), (5);
                  select sum(a) from example`))
            .toEqual([{ sum: 3 }])
    });


    it('supports sum() in inner expression (simple)', () => {
        expect(many(`create table example(a int);
                  insert into example values (1), (2);
                  select 3+sum(a) as sum from example`))
            .toEqual([{ sum: 6 }])
    });

    it('supports sum() in inner expression (grouped by)', () => {
        expect(many(`create table example(id text, a int);
                  insert into example values ('a', 1), ('b', 2), ('a', 3);
                  select id, 3+sum(a) as sum from example group by id`))
            .toEqual([
                { id: 'a', sum: 7 },
                { id: 'b', sum: 5 },
            ]);
    });

    it('supports sum(distinct)', () => {
        expect(many(`create table example(a int);
                    insert into example values (1), (2), (2), (3);
                  select sum(distinct a) from example`))
            .toEqual([
                { sum: 6 },
            ]);
    });

    it('supports aggregation with qualifier', () => {
        expect(many(`create table example(a int);
                  insert into example values (1), (-2), (null), (3), (-4), (5);
                  select pg_catalog.sum(a) from example`))
            .toEqual([{ sum: 3 }]);
    });

    it('throws error when aggregation function doest not exist', () => {
        none(`create table example(a int);
                  insert into example values (1), (-2), (null), (3), (-4), (5);`);

        expect(() => none(`select public.sum(a) from example`)).toThrow(/function public\.sum\(integer\) does not exist/);
    });

    it('sum() returns null when nothing', () => {
        expect(many(`create table example(a int);
                  select sum(a) from example`))
            .toEqual([{ sum: null }]);
    });

    it('supports sum(distinct())', () => {
        expect(many(`create table example(a int);
                insert into example values (1), (1), (null), (2), (2), (3), (-1), (-1);
                select sum(distinct(a)) from example`))
            .toEqual([{ sum: 5 }])
    });

    it('sum(distinct()) returns null when nothing', () => {
        expect(many(`create table example(a int);
                select sum(distinct(a)) from example`))
            .toEqual([{ sum: null }]);
    });

    it.skip('can apply modifier filters on aggregations', () => {
        expect(many(`select
                        sum(v) filter (where v > 1) as f,
                        sum(distinct v) filter (where v > 1) as d,
                        sum(v) as t
                    from (values (1), (2), (2)) as t(v);`))
            .toEqual([{
                f: 4,
                d: 2,
                v: 5,
            }]);
    });

    it('supports array_agg()', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 3), (1, 4);
                    select array_agg(b) from example group by a`))
            .toEqual([{ array_agg: [3, 4] }]);
    });

    it('supports array_agg() with nulls', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 3), (1, null);
                    select array_agg(b) from example group by a`))
            .toEqual([{ array_agg: [3, null] }]);
    });

    it('supports array_agg() with multiple groups', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 3), (1, 4), (2, 5);
                    select array_agg(b) from example group by a`))
            .toEqual([{ array_agg: [3, 4] }, { array_agg: [5] }]);
    });

    it('supports avg()', () => {
        expect(many(`create table example(a int);
                    insert into example values (1), (2);
                    select avg(a) from example`))
            .toEqual([{ avg: 1.5 }]);
    });

    it('supports avg() with nulls', () => {
        expect(many(`create table example(a int);
                    insert into example values (1), (2), (null);
                    select avg(a) from example`))
            .toEqual([{ avg: 1.5 }]);
    });

    it('supports avg() with multiple groups', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 1), (1, 2), (2, 3);
                    select avg(b) from example group by a`))
            .toEqual([{ avg: 1.5 }, { avg: 3 }]);
    });

    it('avg() returns null when nothing', () => {
        expect(many(`create table example(a int);
                    insert into example values (null), (null);
                    select avg(a) from example`))
            .toEqual([{ avg: null }]);
    });

    it('supports avg(distinct)', () => {
        expect(many(`create table example(a int);
                    insert into example values (1), (2), (1);
                    select avg(distinct a) from example`))
            .toEqual([{ avg: 1.5 }]);
    });

    it('supports avg(distinct) with nulls', () => {
        expect(many(`create table example(a int);
                    insert into example values (1), (2), (null), (1);
                    select avg(distinct a) from example`))
            .toEqual([{ avg: 1.5 }]);
    });

    it('supports avg(distinct) with multiple groups', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 1), (1, 2), (1, 1), (2, 3);
                    select avg(distinct b) from example group by a`))
            .toEqual([{ avg: 1.5 }, { avg: 3 }]);
    });

    it('avg(distinct) returns null when nothing', () => {
        expect(many(`create table example(a int);
                    insert into example values (null), (null);
                    select avg(distinct a) from example`))
            .toEqual([{ avg: null }]);
    });


    it('does not return anything when no rows', () => {
        expect(many(`create table test(id text, a int);
                    select id, sum(a) as sum from test group by id`))
            .toEqual([]);
    });


    it('[bugfix] fix min() on timestamptz', () => {
        // checks https://github.com/oguimbal/pg-mem/issues/162
        none(`create table books(name text, created_at timestamptz);
                        SELECT name FROM books WHERE created_at = (SELECT MIN(created_at) FROM books);`);
    });

    describe('jsonb_agg', () => {
        it('supports jsonb_agg', () => {
            expect(many(`select jsonb_agg(col)  from (values ('a'), ('b')) t(col)`))
                .toEqual([{ jsonb_agg: ['a', 'b'] }]);
        })

        it('supports json_agg(distinct) on deep structures', () => {
            expect(many(`select jsonb_agg(distinct col)  from (values ('[1]'::jsonb), ('[1]'::jsonb), ('[2]'::jsonb)) t(col)`))
                .toEqual([{ jsonb_agg: [[1], [2]] }]);
        });
    })

    describe('bool_or / bool_and', () => {
        it('bool_or accepts nulls', () => {
            expect(many(`select bool_or(a) from (values (true), (null), (false)) as t(a)`))
                .toEqual([{ bool_or: true }]);
        });

        it('bool_and accepts nulls', () => {
            expect(many(`select bool_and(a) from (values (true), (null), (true)) as t(a)`))
                .toEqual([{ bool_and: true }]);
        });

        it('bool_and computes the right value', () => {
            expect(many(`select bool_and(a) from (values (true), (false)) as t(a)`))
                .toEqual([{ bool_and: false }]);
        });


        it('bool_or computes the right value', () => {
            expect(many(`select bool_or(a) from (values (true), (false)) as t(a)`))
                .toEqual([{ bool_or: true }]);
        });
    });
});
