import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('DB restore points', () => {

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


    it('can backup & resotre db', () => {
        simpleDb();
        none(`insert into data(id) values ('value')`);
        const bck = db.backup();
        none(`update data set id='updatd';insert into data(id) values ('other')`);
        bck.restore();
        expect(trimNullish(many(`select * from data`))).toEqual([{ id: 'value' }]);
    });


    it('can backup & resotre db multiple times', () => {
        simpleDb();
        none(`insert into data(id) values ('value')`);
        const bck = db.backup();
        none(`update data set id='updatd';insert into data(id) values ('other')`);
        bck.restore();
        expect(trimNullish(many(`select * from data`))).toEqual([{ id: 'value' }]);
        none(`update data set id='updatd';insert into data(id) values ('other')`);
        bck.restore();
        expect(trimNullish(many(`select * from data`))).toEqual([{ id: 'value' }]);
    });

});