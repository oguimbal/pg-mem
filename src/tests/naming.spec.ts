import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';

describe('Naming & collisions', () => {

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


    it('prevents "table <-> table" collisions', () => {
        none('create table test(a text);');
        assert.throws(() => none('create table test(a text);'), /relation "test" already exists/);
    });

    it('prevents "table <-> index" collisions', () => {
        none('create table test(a text);');
        assert.throws(() => none('create index test on test(a)'), /relation "test" already exists/);
    });

    it('prevents "index <-> table" collisions', () => {
        none(`create table other(a text);
                create index test on other(a);`);
        assert.throws(() => none('create table test(a text);'), /relation "test" already exists/);
    });

    it('prevents "table <-> sequence" collisions', () => {
        none('create table test(a text);');
        assert.throws(() => none('create sequence test'), /relation "test" already exists/);
    });

    it('prevents "sequence <-> table" collisions', () => {
        none('create sequence test');
        assert.throws(() => none('create table test(a text);'), /relation "test" already exists/);
    });

    it('prevents "index <-> sequence" collisions', () => {
        none(`create table other(a text);
            create index test on other(a);`);
        assert.throws(() => none('create sequence test'), /relation "test" already exists/);
    });

    it('prevents "sequence <-> index" collisions', () => {
        none(`create table other(a text);
                create sequence test;`);
        assert.throws(() => none('create index test on other(a);'), /relation "test" already exists/);
    });

    it('prevents droping an unqualified table when overriden by system one', () => {
        none(`create table pg_class(a text);`);
        assert.throw(() => none(`drop table pg_class`), /permission denied: "pg_class" is a system catalog/);
        // allows it when fully qualified
        none(`drop table public.pg_class;`);
    });


    it('overrides local tables by system table', () => {
        expect(many(`create table pg_class (a text);
            insert into public.pg_class values ('a');
            select * from pg_class`))
            .to.deep.equal([]);

        // allows it when fully qualified
        expect(many(`select * from public.pg_class`)).to.deep.equal([{ a: 'a' }]);
    });


    it('accepts alter tables with cased names', () => {
        none(`create table "TeSt" (a text);
                alter table "TeSt" add b text;`)
    })

    it('accepts " in names', () => {
        expect(many(`create table "a""b"(a text);
                    insert into "a""b" values ('42');
                    select * from "a""b";`))
            .to.deep.equal([{ a: '42' }]);
    })
});