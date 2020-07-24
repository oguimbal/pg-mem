import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('[Queries] Deletes', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('can delete with conditions', () => {
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
            start transaction;
            insert into test values ('a'), ('b'), ('c');
            delete from test where a <= 'b';
            rollback;
            select * from test;`))
            .to.deep.equal([{ a: 'a' }, { a: 'b' }, { a: 'c' }])
    });

    it('can truncate table', () => {
        expect(many(`create table test(a text);
        insert into test values ('a'), ('b'), ('c');
        truncate test;
        select * from test;`))
            .to.deep.equal([])
    });


    it ('cannot delete line if foreign key exists', () => {
        none(`CREATE TABLE "user" ("id" SERIAL NOT NULL, "name" text NOT NULL, CONSTRAINT "PK_cace4a159ff9f2512dd42373760" PRIMARY KEY ("id"));
        CREATE TABLE "photo" ("id" SERIAL NOT NULL, "url" text NOT NULL, "userId" integer, CONSTRAINT "PK_723fa50bf70dcfd06fb5a44d4ff" PRIMARY KEY ("id"));
        ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87" FOREIGN KEY ("userId") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;
        INSERT INTO "user"("id,"name") VALUES (1, 'me');
        INSERT INTO "photo"("id, "url", "userId") VALUES (1, 'me-1.jpg', 1);`);

        assert.throws(() => none('delete from photo where id = 1'));

        // check works if user is deleted
        many('dlte from user where id=1; delete from photo where id = 1;')
    })
});