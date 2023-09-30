import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';

describe('Alter table', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
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


    it('removes unreferences column when dropped it (bugfix)', () => {
        expect(many(`CREATE TABLE foo (
            uuid   VARCHAR NOT NULL,
            other text
        );

        SELECT * from information_schema.columns where table_name='foo'; -- This shows uuid as expected

        ALTER TABLE foo DROP COLUMN uuid;

        SELECT column_name from information_schema.columns where table_name='foo'; -- This should not show uuid anymore, but it does`))
            .to.deep.equal([{ column_name: 'other' }])
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

    it('cannot add generated on a nullable column', () => {
        assert.throws(() => none(`create table city(name text, city_id int);
            ALTER TABLE public.city ALTER COLUMN city_id ADD GENERATED ALWAYS AS IDENTITY;`)
            , /column "city_id" of relation "city" must be declared NOT NULL before identity can be added/);
    })

    it('can add generated column', () => {
        // https://github.com/oguimbal/pg-mem/issues/9
        const data = many(`create table city(name text, city_id int not null);
                ALTER TABLE public.city ALTER COLUMN city_id ADD GENERATED ALWAYS AS IDENTITY (
                    SEQUENCE NAME public.city_city_id_seq
                    START WITH 0
                    INCREMENT BY 1
                    MINVALUE 0
                    NO MAXVALUE
                    CACHE 1
                );
                insert into city(name) values ('Paris'), ('London');
                select * from city;`);

        expect(data).to.deep.equal([
            { name: 'Paris', city_id: 0 },
            { name: 'London', city_id: 1 },
        ])
    });

    it('handles "if not exists" when creating a column', () => {
        many(`create table mytable(id text);
            alter table mytable add if not exists other text;
            alter table mytable add if not exists id text;`);
    });


    it('(bugfix) supports droping/recreating indexed column', () => {
        // this used to throw
        none(`create table if not exists users (
            id text not null primary key
        );
        alter table users add email text default null;
        create unique index users_by_email on users ((lower(email)));
        drop index if exists users_by_email;
        alter table users alter column email type jsonb;
        create unique index users_by_email on users ((email->>'sha256'));`)
    });

    it('can insert values referring to renamed column', () => {
        none(`create table test("id" integer not null default 1, "col" character varying, constraint "PK" primary key ("id"));
                  alter table test RENAME column "col" TO "newcol";
                  insert into test(id, newcol) values (default, '1') RETURNING "id";
        `);
    });
});
