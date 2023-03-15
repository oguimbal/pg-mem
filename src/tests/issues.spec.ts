import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventCataJoin, preventSeqScan } from './test-utils';

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

    describe('#306', () => {
        // explanation: cannot use index on the right part of a join with the current implementation
        // (see fix in the same commit as this comment)
        function expectResults() {

            none(`
            insert into foo values('2c8ae58d-7d5a-47f4-b8c2-ee61154a46bd',null,2);
            insert into foo values('3c7c772e-bb92-414e-8c83-44f50fbf43ec',null,3);`);

            expect(many(`select  r.id rid, lr.id lrid  from foo r
                    left join foo lr on lr.sub_id = r.id
                    where lr.id is null;`)).to.deep.equal([
                { rid: '2c8ae58d-7d5a-47f4-b8c2-ee61154a46bd', lrid: null },
                { rid: '3c7c772e-bb92-414e-8c83-44f50fbf43ec', lrid: null },
            ]);

            expect(many(`select r.* from foo r
                    left join foo lr on lr.sub_id = r.id
                    where lr.id is null;`)).to.deep.equal([
                { id: '2c8ae58d-7d5a-47f4-b8c2-ee61154a46bd', sub_id: null, bar: 2 },
                { id: '3c7c772e-bb92-414e-8c83-44f50fbf43ec', sub_id: null, bar: 3 },
            ]);

            expect(many(`select lr.* from foo r
                    left join foo lr on lr.sub_id = r.id
                    where lr.id is null;`)).to.deep.equal([
                { id: null, sub_id: null, bar: null },
                { id: null, sub_id: null, bar: null },
            ]);

            expect(many(`select r.* from foo r
                inner join foo lr on lr.sub_id = r.id
                where lr.id is null;`)).to.have.length(0);
        }

        it('with index', () => {
            db.createSchema('test');
            none(`CREATE TABLE "foo" ("id" uuid NOT NULL, "sub_id" uuid, "bar" integer NOT NULL, CONSTRAINT "pk_xxx" PRIMARY KEY ("id"));`);
            expectResults();
        });


        it('without index', () => {
            db.createSchema('test');
            none(`CREATE TABLE "foo" ("id" uuid NOT NULL, "sub_id" uuid, "bar" integer NOT NULL);`);
            expectResults();
        });



        it('can use index on left part', () => {
            db.createSchema('test');
            none(`CREATE TABLE "foo" ("id" uuid NOT NULL, "sub_id" uuid, "bar" integer NOT NULL, CONSTRAINT "pk_xxx" PRIMARY KEY ("id"));
            create index on foo(sub_id);`);

            none(`
            insert into foo values('2c8ae58d-7d5a-47f4-b8c2-ee61154a46bd',null,2);
            insert into foo values('3c7c772e-bb92-414e-8c83-44f50fbf43ec',null,3);`);

            preventSeqScan(db);
            preventCataJoin(db);

            expect(many(`select  r.id rid, lr.id lrid  from foo r
                    left join foo lr on lr.sub_id = r.id
                    where r.id = '2c8ae58d-7d5a-47f4-b8c2-ee61154a46bd';`)).to.deep.equal([
                { rid: '2c8ae58d-7d5a-47f4-b8c2-ee61154a46bd', lrid: null },
            ]);

        });

    })
});
