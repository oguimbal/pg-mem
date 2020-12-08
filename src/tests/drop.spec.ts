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

    it('cannot drop sequence when exists but is not a sequence', () => {
        none(`create table test(a text)`);
        assert.throws(() => none(`drop sequence test;`), /"test" is not a sequence/);
    });


    it ('can drop index', () => {
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
});