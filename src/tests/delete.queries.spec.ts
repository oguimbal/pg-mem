import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';

describe('Deletes', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('can delete with conditions simple', () => {
        expect(many(`create table test(a text);
            insert into test values ('a'), ('b'), ('c');
            delete from test where a <= 'b';
            select * from test;`))
            .to.deep.equal([{ a: 'c' }])
    });

    it('can delete with conditions within transaction', () => {
        expect(many(`create table test(a text);
            start transaction;
            insert into test values ('a'), ('b'), ('c');
            delete from test where a <= 'b';
            commit;
            select * from test;`))
            .to.deep.equal([{ a: 'c' }])
    });


    it('does not delete if rollback transaction', () => {
        expect(many(`create table test(a text);
            insert into test values ('a'), ('b'), ('c');
            commit;
            start transaction;
            delete from test where a <= 'b';
            rollback;
            select * from test;`))
            .to.deep.equal([{ a: 'a' }, { a: 'b' }, { a: 'c' }])
    });

    it('delete if in same rollbacked transaction', () => {
        expect(many(`create table test(a text);
            commit;
            start transaction;
            insert into test values ('a'), ('b'), ('c');
            delete from test where a <= 'b';
            rollback;
            select * from test;`))
            .to.deep.equal([])
    });

    it('can truncate table', () => {
        expect(many(`create table test(a text);
        insert into test values ('a'), ('b'), ('c');
        truncate test;
        select * from test;`))
            .to.deep.equal([])
    });


    it('cannot query primary key condition after truncate', () => {
        // this was a bug
        expect(many(`create table test(a text primary key);
        insert into test values ('a'), ('b'), ('c');
        truncate test;
        select * from test where a='a';`))
            .to.deep.equal([])
    });


    it('cannot query index condition after truncate', () => {
        // this was a bug
        expect(many(`create table test(a text);
        create index on test(a);
        insert into test values ('a'), ('b'), ('c');
        truncate test;
        select * from test where a='a';`))
            .to.deep.equal([])
    });
});