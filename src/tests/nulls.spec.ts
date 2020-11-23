import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { preventSeqScan } from './test-utils';
import { Types } from '../datatypes';
import { _IDb } from '../interfaces-private';

describe('[Queries] Null values', () => {

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
                constraint: { type: 'primary key' },
            }, {
                name: 'str',
                type: Types.text(),
            }, {
                name: 'otherStr',
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
});