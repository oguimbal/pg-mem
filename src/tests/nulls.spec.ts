import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { preventSeqScan } from './test-utils';
import { Types } from '../datatypes';
import { _IDb } from '../interfaces-private';

describe('Null values', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'test') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    function simpleDb() {
        db.public.declareTable({
            name: 'data',
            fields: [{
                name: 'id',
                type: Types.text(),
                constraints: [{ type: 'primary key' }],
            }, {
                name: 'str',
                type: Types.text(),
            }, {
                name: 'otherstr',
                type: Types.text(),
            }],
        });
        return db;
    }

    function setupNulls(withIndex = true) {
        db = simpleDb()
        if (withIndex) {
            none('create index on data(str);')
        }
        none(`insert into data(id, str) values ('id1', null);
                insert into data(id, str) values ('id2', 'notnull2');
                insert into data(id, str) values ('id3', null);
                insert into data(id, str) values ('id4', 'notnull4')`);
        return db;
    }

    it('returns nothing when using index on =null', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = many('select * from data where str = null');
        expect(got).to.deep.equal([]);
    });

    it('returns nothing when using index on !=null', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = many('select * from data where str != null');
        expect(got).to.deep.equal([]);
    });

    it('returns nothing when seqscan on =null', () => {
        setupNulls(false);
        const got = many('select * from data where str = null');
        expect(got).to.deep.equal([]);
    });

    it('returns nothing when seqscan on !=null', () => {
        setupNulls(false);
        const got = many('select * from data where str != null');
        expect(got).to.deep.equal([]);
    });

    it('returns something on isnull when setting to default', () => {
        expect(many(`create table test(id text primary key, val text);
            insert into test values ('a', default);
            select * from test where val isnull`))
            .to.deep.equal([{ id: 'a', val: null }]);
    });

    it('returns nothing on notnull when setting to default', () => {
        expect(many(`create table test(id text primary key, val text);
            insert into test values ('a', default);
            select * from test where val notnull`))
            .to.deep.equal([]);
    });

    it('can select jsonb null', () => {
        expect(many(`drop table if exists test;
        create table test(txt text);
                        insert into test values ('null'), (null);
                        select txt::jsonb as casted, txt::jsonb is null as "castedIsNil", txt::jsonb = null as "eqNil", txt::jsonb = 'null'::jsonb as "eqNilJson" from test;`))
            .to.deep.equal([
                { casted: null, castedIsNil: false, eqNil: null, eqNilJson: true },
                { casted: null, castedIsNil: true, eqNil: null, eqNilJson: null },
            ]);

        expect(many(`select 'null'::jsonb`))
            .to.deep.equal([{ jsonb: null }]);

        expect(many(`select concat('nu','ll')::jsonb`))
            .to.deep.equal([{ jsonb: null }]);

        expect(many(`select val, val isnull as "isNil", val = null as "eqNil", val = 'null'::jsonb as "eqNilJson" from (values ('{"abc":null}'::jsonb -> 'abc')) as tbl(val)`))
            .to.deep.equal([{
                val: null,
                isNil: false,
                eqNil: null,
                eqNilJson: true,
            }])
    })
});