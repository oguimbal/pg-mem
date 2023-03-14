import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('Github issues', () => {

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



    it('#300', () => {
        none(`CREATE TABLE "test_record" ("id" uuid, "name" character varying NOT NULL, "testNumber" numeric(18,2), CONSTRAINT "PK_78ddda202d30c6ccbb2b528a0ac" PRIMARY KEY ("id"))`);
    });

    it('#306 with index', () => {
        db.createSchema('test');
        expect(many(`CREATE TABLE "test"."foo" ("id" uuid NOT NULL, "sub_id" uuid, "bar" integer NOT NULL);
        insert into test.foo values('2c8ae58d-7d5a-47f4-b8c2-ee61154a46bd',null,2);
        insert into test.foo values('3c7c772e-bb92-414e-8c83-44f50fbf43ec',null,3);
        select r.* from test.foo r
                left join test.foo lr on lr.sub_id = r.id
                where lr.id is null;`)).to.have.length(2);

        expect(many(`select r.* from test.foo r
            inner join test.foo lr on lr.sub_id = r.id
            where lr.id is null;`)).to.have.length(0);
    });

    it('#306 without index', () => {
        db.createSchema('test');
        expect(many(`CREATE TABLE "test"."foo" ("id" uuid NOT NULL, "sub_id" uuid, "bar" integer NOT NULL);
        insert into test.foo values('2c8ae58d-7d5a-47f4-b8c2-ee61154a46bd',null,2);
        insert into test.foo values('3c7c772e-bb92-414e-8c83-44f50fbf43ec',null,3);
        select r.* from test.foo r
                left join test.foo lr on lr.sub_id = r.id
                where lr.id is null;`)).to.have.length(2);

        expect(many(`select r.* from test.foo r
            inner join test.foo lr on lr.sub_id = r.id
            where lr.id is null;`)).to.have.length(0);
    });
});
