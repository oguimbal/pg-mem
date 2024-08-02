import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { IMemoryDb } from '../interfaces';
import { Types } from '../datatypes';

describe('Transactions', () => {

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

    it('can rollback an update', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        expect(many(`update data set str='to rollback';
                     rollback;
                     select str from data;`))
            .toEqual([{ str: 'some str' }]);
    });


});