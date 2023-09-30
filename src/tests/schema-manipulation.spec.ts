import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { preventSeqScan } from './test-utils';
import moment from 'moment';
import { MemoryTable } from '../table';

describe('Schema manipulation', () => {

    it('table with primary', () => {
        const db = newDb();
        db.public.none(`create table test(id text primary key, value text)`);
        preventSeqScan(db, 'test');
        db.public.none(`insert into test(id, value) values ('A', 'Value A')`);
        const many = db.public.many(`select value from test where id='A'`);
        expect(many).to.deep.equal([{ value: 'Value A' }]);

        // check that cannot be null
        const idCol = (db.public.getTable('test') as MemoryTable).getColumnRef('id');
        assert.isTrue(idCol.notNull);

        //check that can add not null constraint
        db.public.none(`alter table test alter column id set not null`);
        db.public.none(`alter table test alter column id set not null`);
    });

    it('table without primary', () => {
        const db = newDb();
        db.public.none(`create table test(value text)`);
        db.public.none(`insert into test(value) values ('Value A')`);
        const many = db.public.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 'Value A' }]);
    });

    it('table int', () => {
        const db = newDb();
        db.public.none(`create table test(value int)`);
        db.public.none(`insert into test(value) values (42.5)`);
        const many = db.public.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 43 }]); // must be rounded (int)
    });

    it('table integer', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value integer);
                        insert into test(value) values (42.5);
                        select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 43 }]); // must be rounded (int)
    });

    it('table varchar', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value varchar);
                        insert into test(value) values ('test');
                        select value from test where value is not null;`);
        expect(many).to.deep.equal([{ value: 'test' }]);
    });

    it('table varchar(n)', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value varchar(5));
                        insert into test(value) values ('test');
                        select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 'test' }]);
    });

    it('table varchar(n) with insert too long', () => {
        const db = newDb();
        db.public.none(`create table test(value varchar(5))`);
        assert.throws(() => {
            db.public.none(`insert into test(value) values ('12345678')`);
        });
    });


    it('table float', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value float);
                        insert into test(value) values (42.5);
                        select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42.5 }]);
    });

    it('table decimal', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value decimal);;
                                    insert into test(value) values (42.5);
                                    select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42.5 }]);
    });


    it('table timestamp', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value timestamp);
                                    insert into test(value) values ('2000-01-02 03:04:05');
                                    select * from test;`);
        expect(many.map(x => x.value instanceof Date)).to.deep.equal([true]);
        expect(many.map(x => moment.utc(x.value)?.format('YYYY-MM-DD HH:mm:ss'))).to.deep.equal(['2000-01-02 03:04:05']);
    });

    it('table timestamp with time zone', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value timestamp with time zone);
                                    insert into test(value) values ('2000-01-02 03:04:05');
                                    select * from test;`);
        expect(many.map(x => x.value instanceof Date)).to.deep.equal([true]);
        expect(many.map(x => moment.utc(x.value)?.format('YYYY-MM-DD HH:mm:ss'))).to.deep.equal(['2000-01-02 03:04:05']);
    });

    it('table timestamp without time zone', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value timestamp without time zone);
                                    insert into test(value) values ('2000-01-02 03:04:05');
                                    select * from test;`);
        expect(many.map(x => x.value instanceof Date)).to.deep.equal([true]);
        expect(many.map(x => moment.utc(x.value)?.format('YYYY-MM-DD HH:mm:ss'))).to.deep.equal(['2000-01-02 03:04:05']);
    });

    it('table date', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value date);
                                    insert into test(value) values ('2000-01-02 03:04:05');
                                    select * from test;`);
        expect(many.map(x => x.value instanceof Date)).to.deep.equal([true]);
        expect(many.map(x => moment.utc(x.value)?.format('YYYY-MM-DD HH:mm:ss'))).to.deep.equal(['2000-01-02 00:00:00']);
    });


    it('table date array', () => {
        const db = newDb();
        const { value } = db.public.one(`create table test(value date[]);
                                    insert into test(value) values ('{2000-01-02 03:04:05,2000-01-02 03:04:05}');
                                    select * from test;`);
        if (!Array.isArray(value)) {
            assert.fail('should be array');
        }
        expect(value.map(y => moment.utc(y)?.format('YYYY-MM-DD HH:mm:ss')))
            .to.deep.equal(['2000-01-02 00:00:00', '2000-01-02 00:00:00']);
    });

    it('table jsonb', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value jsonb);
                                    insert into test(value) values ('{"a": 42}');
                                    select * from test;`);
        expect(many).to.deep.equal([{ value: { a: 42 } }]);
    });


    it('table json', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value json);
                                    insert into test(value) values ('{"a": 42}');
                                    select * from test;`);
        expect(many).to.deep.equal([{ value: { a: 42 } }]);
    });

    it('table invalid jsonb', () => {
        const db = newDb();
        db.public.none(`create table test(value jsonb);`);
        assert.throws(() => db.public.none(`insert into test(value) values ('{"a" 42}');`));
    });

    it('bugfix 1', () => {
        const db = newDb();
        db.public.none('CREATE TABLE "a" ("id" character varying NOT NULL, "b" jsonb NOT NULL, "c" jsonb NOT NULL, "d" character varying NOT NULL, "e" jsonb NOT NULL, "f" TIMESTAMP NOT NULL, "g" character varying NOT NULL, "h" jsonb NOT NULL, "i" jsonb NOT NULL, "j" jsonb NOT NULL, "k" jsonb NOT NULL, "l" jsonb NOT NULL, "m" character varying NOT NULL, "n" jsonb NOT NULL, "o" jsonb NOT NULL, "p" jsonb NOT NULL, "q" jsonb NOT NULL, "r" character varying NOT NULL, "s" jsonb NOT NULL, "t" jsonb NOT NULL, "u" TIMESTAMP NOT NULL, "v" jsonb NOT NULL, "w" text NOT NULL, "x" text NOT NULL, "y" TIMESTAMP NOT NULL, "z" jsonb NOT NULL, CONSTRAINT "PK_e2f1f4741f2094ce789b0a7c5b3" PRIMARY KEY ("id"));');
    })

    it('bugfix 2', () => {
        const db = newDb();
        db.public.none('CREATE TABLE "a" ("id" character varying NOT NULL, "b" text NOT NULL, "c" character varying NOT NULL, "d" jsonb array NOT NULL, "e" jsonb NOT NULL, CONSTRAINT "PK_17c3a89f58a2997276084e706e8" PRIMARY KEY ("id"));');
    })

    it('fix: create table with cased constraint on column', () => {
        newDb().public.none(` CREATE TABLE "my_knowledge_category" (
            "id" character varying NOT NULL,
            "knowledgeApplication" character varying NOT NULL,
            "knowledgeLanguage" character varying NOT NULL,
            CONSTRAINT "PK_fca04a81c9ec0f8d9527180c80c" PRIMARY KEY ("id", "knowledgeApplication", "knowledgeLanguage")
          );`);
    })
});
