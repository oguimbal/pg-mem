import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { watchUse } from '../utils';
import { preventSeqScan } from './test-utils';

describe('Conversions', () => {

    it('varchar(n) with insert too long', () => {
        const db = newDb();
        db.public.none(`create table test(value varchar(5))`);
        assert.throws(() => {
            db.public.none(`insert into test(value) values ('12345678')`);
        });
    });

    it('compatible decimal with string', () => {
        const db = newDb();
        db.public.none(`create table test(value decimal)`);
        db.public.none(`insert into test(value) values ('42.5')`);
        const many = db.public.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42.5 }]);
    });

    it('incompatible decimal with string', () => {
        const db = newDb();
        db.public.none(`create table test(value decimal)`);
        assert.throws(() => db.public.none(`insert into test(value) values ('blah')`));
    });

    it('compatible int with string', () => {
        const db = newDb();
        db.public.none(`create table test(value int)`);
        db.public.none(`insert into test(value) values ('42')`);
        const many = db.public.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42 }]);
    });

    it('incompatible int with string', () => {
        const db = newDb();
        db.public.none(`create table test(value int)`);
        assert.throws(() => db.public.none(`insert into test(value) values ('42.5')`));
    });
});
