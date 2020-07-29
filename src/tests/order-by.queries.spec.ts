import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('[Queries] Order by', () => {

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

    it('simple order by asc', () => {
        expect(many(`create table test(val text);
            insert into test values ('b'), ('a'), (null);
            select * from test order by val`))
            .to.deep.equal([
                { val: 'a' }
                , { val: 'b' }
                , { val: null }
            ]);
    });

    it('simple order by desc', () => {
        expect(many(`create table test(val text);
            insert into test values ('b'), ('a'), (null);
            select * from test order by val desc`))
            .to.deep.equal([
                { val: null }
                , { val: 'b' }
                , { val: 'a' }
            ]);
    });

    it('order by two columns', () => {
        expect(many(`create table test(a integer, b integer);
            insert into test values (1, 13), (2, 11), (1, null), (1, 11), (2, 12), (1, 12), (null, 1), (null, 5);
            select * from test order by a, b desc`))
            .to.deep.equal([
                { a: 1, b: null }
                , { a: 1, b: 13 }
                , { a: 1, b: 12 }
                , { a: 1, b: 11 }

                , { a: 2, b: 12 }
                , { a: 2, b: 11 }

                , { a: null, b: 5 }
                , { a: null, b: 1 }
            ]);
    });
});