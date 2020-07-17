import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('Operators', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.query.many.bind(db.query);
        none = db.query.none.bind(db.query);
    });

    it('+ on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (1, 2);
                            select a+b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([3]);
    });

    it('- on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 1);
                            select a-b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([1]);
    });

    it('/ on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (17, 10);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([1]); // trunc is used on divisions
    });

    it('/ on neg ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (-17, 10);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([-1]); // trunc is used on divisions
    });

    it('/ on floats', () => {
        const result = many(`create table test(a float, b float);
                            insert into test values (5, 2);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([2.5]);
    });
    it('/ on float and int', () => {
        const result = many(`create table test(a float, b int);
                            insert into test values (5, 2);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([2.5]);
    });
    it('* on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (4, 2);
                            select a*b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([8]);
    });
});
