import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('[Queries] Alter table', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    function simpleDb() {
        none(`create table test(a text);
        insert into test values ('a')`)
    }

    it('can rename table', () => {
        simpleDb();
        expect(many(`
        alter table test rename to newtable;
        select * from newtable;
        `)).to.deep.equal([{ a: 'a' }])
    });

    it('can rename column', () => {
        simpleDb();
        expect(many(`
        alter table test rename column a to b;
        select * from test;
        `)).to.deep.equal([{ b: 'a' }])
    });
    it('cannot rename override column', () => {
        assert.throws(() => many(`
        create table test(a text, b text);
        alter table test rename column a to b;
        `));
    });

    it('can add column', () => {
        simpleDb();
        expect(many(`alter table test add column b text;
            select * from test;`))
            .to.deep.equal([{ a: 'a', b: null }])
    });

    it('cannot add an existing column', () => {
        simpleDb();
        assert.throws(() => none('alter table test add column a text;'));
    })


    it('skips column add if exists', () => {
        simpleDb();
        expect(many(`alter table test add column if not exists a text;
            select * from test;`))
            .to.deep.equal([{ a: 'a' }])
    });

    it('cannot add not null column without default', () => {
        simpleDb();
        assert.throws(() => many(`alter table test add column b text not null`));
    });

    it('can add not null column with default', () => {
        simpleDb();
        expect(many(`alter table test add column b text not null default 'test';
            select * from test;`))
            .to.deep.equal([{ a: 'a', b: 'test' }])
    });

    it('drop column', () => {
        expect(many(`create table test(a text, b text);
            insert into test values ('a', 'b');
            alter table test drop column b;
            select * from test;`))
            .to.deep.equal([{ a: 'a' }])
    });

    it('set default', () => {
        expect(many(`create table test(a text, b text);
        alter table test alter b set default 'x';
        insert into test(a) values ('a');
        select * from test;`))
            .to.deep.equal([{ a: 'a', b: 'x' }])
    });

    it('drop default', () => {
        expect(many(`create table test(a text, b text);
        alter table test alter b set default 'x';
        insert into test(a) values ('a1');
        alter table test alter b drop default;
        insert into test(a) values ('a2');
        select * from test;`))
            .to.deep.equal([{ a: 'a1', b: 'x' }, { a: 'a2', b: null }])
    });


    it('set not null prevents inserting nulls', () => {
        simpleDb();
        assert.throws(() => many(`alter table test alter a set not null;
        insert into test(a) values (null);`));
    });

    it('nulls prevents setting not null constraint', () => {
        simpleDb();
        assert.throws(() => many(`
            insert into test(a) values (null);
            alter table test alter a set not null;`));
    });

    it('drop not null allows inserting nulls', () => {
        simpleDb();
        expect(many(`alter table test alter a set not null;
        alter table test alter a drop not null;
        insert into test(a) values (null);
        select * from test`))
            .to.deep.equal([{ a: 'a' }, { a: null }])
    });


    it('can drop column part of a multiple index', () => {
        none(`create table test(a text, b text);
                create index on test(a, b);
                alter table test drop a;`);
        expect(db.getTable('test').listIndices())
            .to.deep.equal([]);
    });

    it('can drop column part of its own index', () => {
        none(`create table test(a text, b text);
                create index on test(a);
                alter table test drop a;`);
        expect(db.getTable('test').listIndices())
            .to.deep.equal([]);
    });
});
