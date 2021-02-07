import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
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
            .to.deep.equal([
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
            .to.deep.equal([
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
            .to.deep.equal([
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
            .to.deep.equal([
                { a: 'a', b: 'b' },
            ]);
    });

    it('select distinct on two, select something else', () => {
        expect(many(`create table data(a text, b text, c text);
            insert into data values ('a', 'b', '1');
            insert into data values ('a', 'b', '2');
            insert into data values ('a', 'c', '3');
            select distinct on (a,b) c from data`))
            .to.deep.equal([
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
            .to.deep.equal([
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
            .to.deep.equal([
                { x: 'b', count: 2 },
                { x: 'c', count: 2 },
            ])
    });

    it('behaves nicely with nulls on multiple distinct', () => {
        expect(many(`create table test(v jsonb, i int);
                    insert into test values ('{}',0),  ('[]',1), ('{}',0), ('[]',null), (null, 1);
                    select distinct v, i from test order by v,i desc;`))
            .to.deep.equal([
                { v: [], i: null },
                { v: [], i: 1 },
                { v: {}, i: 0 },
                { v: null, i: 1 },
            ])
    });

});
