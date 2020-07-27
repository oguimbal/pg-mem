import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('DB forking', () => {

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

    it('can fork db', () => {
        simpleDb();
        none(`insert into data(id) values ('value')`);
        const ndb = db.fork();
        db.public.none(`insert into data(id) values ('db1')`);
        ndb.public.none(`insert into data(id) values ('db2')`);
        expect(db.public.many(`select * from data`)).to.deep.equal([{ id: 'value' }, { id: 'db1' }]);
        expect(ndb.public.many(`select * from data`)).to.deep.equal([{ id: 'value' }, { id: 'db2' }]);
    });


    it('can backup & resotre db', () => {
        simpleDb();
        none(`insert into data(id) values ('value')`);
        const bck = db.backup();
        none(`insert into data(id) values ('other')`);
        bck.restore();
        expect(many(`select * from data`)).to.deep.equal([{ id: 'value' }]);
    });

});