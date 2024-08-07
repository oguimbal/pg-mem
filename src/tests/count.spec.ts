import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';
import { expectQueryError, preventSeqScan } from './test-utils';

describe('Count', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    it('selects simple count', () => {
        expect(one(`create table test(val text);
            insert into test values ('a'), ('b'), (null);
            select count(*) as cnt from test`))
            .toEqual({ cnt: 3 });
    });

    it('selects simple count in expression', () => {
        expect(one(`create table test(val text);
            insert into test values ('a'), ('b'), (null);
            select count(*)+1 as cnt from test`))
            .toEqual({ cnt: 4 });
    });



    it('counts nulls the right way', () => {
        expect(one(`create table test(val int, val2 int);
        insert into test values (1, null), (null, 1), (1, 1), (2, 3), (null, null);
        select count(COALESCE(val,val2)) as a, COUNT(val+val2) as b, count(*) as c from test;`))
            .toEqual({ a: 4, b: 2, c: 5 });
    });


    it('counts on group by the right way', () => {
        expect(many(`create table test(val int, val2 int);
                        insert into test values (1, null), (null, 1), (1, 1), (2, 3), (null, null);
                        select count(val2) as val2cnt, count(val) as valCnt,val from test group by val;`))
            .toEqual([
                { val2cnt: 1, valcnt: 2, val: 1 }
                , { val2cnt: 1, valcnt: 0, val: null }
                , { val2cnt: 1, valcnt: 1, val: 2 }
            ])
    })

    it('can use index on expression to count', () => {
        preventSeqScan(db);
        expect(many(`create table test(a int, b int);
                        create index on test((a+b));
                        insert into test values (1, 1), (2, 0), (3, 0), (4, 0), (4, null);
                        select count(*) as cnt, b+a as g from test group by a+b;`))
            .toEqual([
                { g: 2, cnt: 2 }
                , { g: 3, cnt: 1 }
                , { g: 4, cnt: 1 }
                , { g: null, cnt: 1 }
            ])
    })

    it('count on empty table', () => {
        expect(one(`create table test(val text);
            select count(val) as cnt from test`))
            .toEqual({ cnt: 0 });
    });

    it('count on empty selection', () => {
        expect(one(`create table test(id int, val text);
            insert into test values (1, 'a');
            select count(*) as cnt from test where id = 2`))
            .toEqual({ cnt: 0 });
    });



    it('can count distinct single value', () => {
        expect(one(`create table test(val int);
            insert into test values (0), (1), (1), (2), (3), (1);
            select count (distinct(val)) from test where val > 0`))
            .toEqual({ count: 3 });
    });


    it('can count distinct multiple value', () => {
        expect(one(`create table test(a int, b int);
            insert into test values (0, 0), (1, 0), (1, 1), (1, 1), (2, 0), (0, 1), (2, 0);
            select count (distinct(a, b)) from test`))
            .toEqual({ count: 5 });
    });

    it('can select multiple counts', () => {
        expect(one(`create table test(a int, b int);
                insert into test values (0, 0), (1, 0), (1, 1), (1, 1), (2, 0), (0, 1), (2, 0);
                select count (*) as a, count(distinct(a, b)) as b from test`))
            .toEqual({ a: 7, b: 5 });
    });

    it('cannot count distinct *', () => {
        one(`create table test(val int);
            insert into test values (0), (1), (1), (2), (3), (1);`);
        expectQueryError(() => one(`select count (distinct(*)) from test`));
    });

    it('distincts jsonb values', () => {
        expect(one(`create table test(v jsonb);
                    insert into test values ('{}'), ('{}'), ('[]');
                    select count(distinct(v)) from test;`))
            .toEqual({ count: 2 });
    });



    it('ignores null on count distinct jsonb values', () => {
        expect(one(`create table test(v jsonb);
                    insert into test values ('{}'), ('{}'), ('[]'), (null);
                    select count(distinct(v)) from test;`))
            .toEqual({ count: 2 });
    })

    it('behaves nicely with nulls on multiple count', () => {
        expect(one(`create table test(v jsonb, i int);
                    insert into test values ('{}',0), ('{}',0), ('[]',null), (null, 1);
                    select count(distinct(v,i)) from test;`))
            .toEqual({ count: 3 });
        expect(one(`insert into test values (null, null);
                        select count(distinct(v,i)) from test;`))
            .toEqual({ count: 4 });
    });
});
