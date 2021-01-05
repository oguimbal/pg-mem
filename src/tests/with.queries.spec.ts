import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { Types } from '../datatypes';
import { _IDb } from '../interfaces-private';
import { SelectStatement } from 'pgsql-ast-parser';
import { buildValue } from '../predicate';
import { parseSql } from '../parse-cache';

describe('With statement', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('can use select in select', () => {
        expect(many(`create table data(a text);
            insert into data values ('a'), ('b'), ('c');
            WITH sel AS (select * from data where a != 'a')
            SELECT 'val ' || s.a as val from sel s;`))
            .to.deep.equal([
                { val: 'val b' },
                { val: 'val c' }
            ])
    });

    it('can use delete result multiple times in select', () => {
        expect(many(`create table data(a text);
                    insert into data values ('a'), ('b'), ('c');
                    WITH sel AS (delete from data where a='a' returning a)
                    SELECT 'val ' || s.a from sel s union (select * from sel);`))
            .to.deep.equal([
                { column: 'val a' },
                { column: 'a' },
            ])
    });

    it('cannot use with as a table', () => {
        assert.throws(() => many(`create table data(a text);
            WITH sel AS (select * from data)
            SELECT 'sel'::regclass;`), /relation "sel" does not exist/);
    });

    it('only inserts once with statement is executed', () => {
        expect(many(`create table data(a text);
            insert into data values ('a');
            with test as (insert into data values ('new'))
            select * from data;`))
            .to.deep.equal([{ a: 'a' }]);
        expect(many(`select * from data`))
            .to.deep.equal([{ a: 'a' }, { a: 'new' }]);
    });

    it ('must have a returning clause when used', () => {
        assert.throws(() => many(`create table data(a text);
            with test as (insert into data values ('x'))
            select * from test;`), /WITH query "test" does not have a RETURNING clause/)
    });

    it ('must not be able to override "with" aliases', () => {
        assert.throws(() => many(`create table data(a text);
            with test as (insert into data values ('x')), test as (insert into data values ('x'))
            select * from data;`), /WITH query name "test" specified more than once/)
    })
});