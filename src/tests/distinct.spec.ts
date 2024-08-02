import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('Distinct', () => {

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

    it('select distinct columns', () => {
        expect(many(`create table data(a text, b text);
            insert into data values ('a', 'b');
            insert into data values ('a', 'b');
            insert into data values ('a', 'c');
            select distinct a,b from data`))
            .toEqual([
                { a: 'a', b: 'b' },
                { a: 'a', b: 'c' },
            ]);
    });


    it('select distinct does not consider hidden values', () => {
        expect(many(`create table data(a text, b text, c text);
                insert into data values ('a', 'b', '1');
                insert into data values ('a', 'b', '2');
                insert into data values ('a', 'c', '3');
                select distinct a,b from data`))
            .toEqual([
                { a: 'a', b: 'b' },
                { a: 'a', b: 'c' },
            ]);
    });


    it('select distinct *', () => {
        expect(many(`create table data(a text, b text, c text);
        insert into data values ('a', 'b', '1');
        insert into data values ('a', 'b', '2');
        insert into data values ('a', 'c', '3');
        insert into data values ('a', 'c', '3');
        select distinct * from data`))
            .toEqual([
                { a: 'a', b: 'b', c: '1' },
                { a: 'a', b: 'b', c: '2' },
                { a: 'a', b: 'c', c: '3' },
            ]);
    });

    it('select distinct on one', () => {
        expect(many(`create table data(a text, b text);
            insert into data values ('a', 'b');
            insert into data values ('a', 'b');
            insert into data values ('a', 'c');
            select distinct on (a) a,b from data`))
            .toEqual([
                { a: 'a', b: 'b' },
            ]);
    });

    it('select distinct on two, select something else', () => {
        expect(many(`create table data(a text, b text, c text);
            insert into data values ('a', 'b', '1');
            insert into data values ('a', 'b', '2');
            insert into data values ('a', 'c', '3');
            select distinct on (a,b) c from data`))
            .toEqual([
                { c: '1' },
                { c: '3' },
            ]);
    });

    it('can distinct on count when grouping by', () => {
        expect(many(`create table data(a text, b text, c text);
        insert into data values ('a', 'b', '1');
        insert into data values ('a', 'b', '2');
        insert into data values ('a', 'c', '3');
        insert into data values ('a', 'c', '3');
        select distinct on (count(*)) b, count(*) from data group by b;`))
            .toEqual([
                { b: 'b', count: 2 }
            ])
    });

    // todo
    it.skip('[todo] can distinct on count+key when grouping by and not selecting key', () => {
        expect(many(`create table data(a text, b text, c text);
        insert into data values ('a', 'b', '1');
        insert into data values ('a', 'b', '2');
        insert into data values ('a', 'c', '3');
        insert into data values ('a', 'c', '3');
        select distinct on (b || ' ', count(*)) b as x, count(*) from data group by b;`))
            .toEqual([
                { x: 'b', count: 2 },
                { x: 'c', count: 2 },
            ])
    });

    it('behaves nicely with nulls on multiple distinct', () => {
        expect(many(`create table test(v jsonb, i int);
                    insert into test values ('{}',0),  ('[]',1), ('{}',0), ('[]',null), (null, 1);
                    select distinct v, i from test order by v,i desc;`))
            .toEqual([
                { v: [], i: null },
                { v: [], i: 1 },
                { v: {}, i: 0 },
                { v: null, i: 1 },
            ])
    });

    it('select distinct on one with desc order on two', () => {
        expect(many(`create table data(a text, b text, c text);
            insert into data values ('a', 'b', '1');
            insert into data values ('a', 'b', '2');
            insert into data values ('a', 'c', '3');
            select distinct on (a) c from data order by a, c desc`))
            .toEqual([
                { c: '3' },
            ]);
    });
});
