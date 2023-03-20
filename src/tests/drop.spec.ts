import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';

describe('Drop', () => {

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


    it('can drop table', () => {
        none(`create table test(a text);
            drop table test;`);
        assert.throws(() => none('select * from test'), /relation "test" does not exist/);
    });

    it('cannot drop table when exists but is not a table', () => {
        none(`create sequence test`);
        assert.throws(() => none(`drop table test;`), /"test" is not a table/);
    });


    it('can drop sequence', () => {
        none(`create sequence test;
            SELECT  nextval('public."test"');
            drop sequence test;`);
        assert.throws(() => none(`SELECT  nextval('public."test"')`), /relation "test" does not exist/);
    });


    it('can drop type', () => {
        none(`create type test as enum ('a', 'b');
            drop type test;`);
    });

    it('ignores drop type if not exists', () => {
        none(`drop type if exists test;`);
    });

    it('fails to drop type if not exists', () => {
        assert.throws(() => none(`drop type test;`), /"test" does not exist/);
    })


    // todo: register where types have been used, and drop if any usage.
    it.skip('fails to drop type if used', () => {
        none(`create type test as enum ('a', 'b');
            create table test_table(val test);
        `);
        assert.throws(() => none(`drop type test`), /annot drop type test because other objects depend on it/);
    });


    it('cannot drop sequence when exists but is not a sequence', () => {
        none(`create table test(a text)`);
        assert.throws(() => none(`drop sequence test;`), /"test" is not a sequence/);
    });


    it('can drop index', () => {
        none(`create table test(a text);
            create index idx on test(a);`);

        // check uses index
        const sub = preventSeqScan(db)
        none(`select * from test where a='a';`);
        sub.unsubscribe();

        // drop index
        none(`drop index idx;`)

        // check does not use index anymore
        let seq = false;
        db.on('seq-scan', () => seq = true);
        none(`select * from test where a='a';`);
        assert.isTrue(seq);
    })

    it('cannot drop index when exists but is not an index', () => {
        none(`create table test(a text)`);
        assert.throws(() => none(`drop index test;`), /"test" is not an index/);
    });

    it('can recreate an index after drop', () => {
        none(`create table test(a text);
            create index idx on test(a);
            drop index idx;
            create index idx on test(a);`);
    });


    it('throws an error on ambiguous function drop', () => {
        db.registerLanguage('sql', () => () => assert.fail('not supposed to be called'));
        none(`
            create function my_function(txt text) returns text as $$select '42'$$ language sql;
            create function my_function() returns text as $$select '42'$$ language sql;
            `);
        assert.throws(() => none(`drop function my_function`), /function name "my_function" is not unique/);
    });

    it('throws an error when no function to drop', () => {
        assert.throws(() => none(`drop function my_function`), /could not find a function named "my_function"/);
        assert.throws(() => none(`drop function my_function(text)`), /function my_function\(text\) does not exist/);
    });

    it('accepts a function drop when not existing', () => {
        none(`drop function if exists my_function;`);
    });

    it('drops a function', () => {
        db.registerLanguage('sql', () => () => '42');
        none(`create function my_function() returns text as $$select '42'$$ language sql;
            select my_function();
            drop function my_function;`);

        assert.throws(() => none(`select my_function()`), /function my_function\(\) does not exist/);
    });

    it('drops the right overload', () => {
        db.registerLanguage('sql', code => () => code.code);
        expect(many(`
            create function my_function(txt text) returns text as $$with arg$$ language sql;
            create function my_function() returns text as $$without arg$$ language sql;
            drop function my_function(text);
            select my_function() data;
            `))
            .to.deep.equal([{ data: 'without arg' }])

    });

    it('test dropping unique constraint', async () => {
        db.public.query(
            'CREATE TABLE "table" ("id" character varying NOT NULL, "col" character varying, CONSTRAINT "REL_constraint" UNIQUE ("col"), CONSTRAINT "PK_constraint" PRIMARY KEY ("id"))',
        );
        db.public.query('ALTER TABLE "table" DROP CONSTRAINT "REL_constraint"');
        db.public.query('ALTER TABLE "table" DROP COLUMN "col"');
    });
});
