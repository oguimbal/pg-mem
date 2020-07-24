import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';
import { Types } from '../datatypes';

describe('[Queries] Transactions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'test') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    function simpleDb() {
        db.public.declareTable({
            name: 'data',
            fields: [{
                id: 'id',
                type: Types.text(),
                primary: true,
            }, {
                id: 'str',
                type: Types.text(),
            }, {
                id: 'otherStr',
                type: Types.text(),
            }],
        });
        return db;
    }

    it('can rollback an update', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        expect(many(`update data set str='to rollback';
                     rollback;
                     select str from data;`))
            .to.deep.equal([{ str: 'some str' }]);
    });


});