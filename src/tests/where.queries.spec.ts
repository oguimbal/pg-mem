import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('Where', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
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

    it('where on primary', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id) values ('some value')`);
        let got = many(`select * from data where id='SOME ID'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where id='some value'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);

    });


    it('where constant true', () => {
        simpleDb();
        none(`insert into data(id) values ('some value')`);
        let got = many('select * from data where 1 = 1');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);
    });

    it('where constant false', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id) values ('some value')`);
        let got = many('select * from data where 1 = 0');
        expect(trimNullish(got)).to.deep.equal([]);
    });

    it('where on other', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where str='none'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where str='some str'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });

    it('call lower in condition', () => {
        simpleDb();
        none(`insert into data(id, str) values ('id1', 'SOME STRING'), ('id2', 'other string'), ('id3', 'Some String')`);
        const result = many(`select id from data where lower(str)='some string'`);
        expect(result.map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });


});