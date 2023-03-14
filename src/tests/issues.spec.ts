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
});
