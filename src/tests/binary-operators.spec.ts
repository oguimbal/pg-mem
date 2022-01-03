import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces-private';

describe('Binary operators', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let one: (str: string) => any;
    function all(table = 'test') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });



    it('[bugfix] can substract date & ints', () => {
        // was throwing (cannot cast type integer to date)
        //  => https://github.com/oguimbal/pg-mem/issues/172
        assert.instanceOf(one(`select CURRENT_DATE - 7 as dt`).dt, Date);
        assert.instanceOf(one(`select CURRENT_DATE + 7 as dt`).dt, Date);
        assert.instanceOf(one(`select 7 + CURRENT_DATE as dt`).dt, Date);

        // only works with ints
        assert.throws(() => many(`select 7 - CURRENT_DATE`), /operator does not exist: integer - date/);
    })

    it.skip('should not be able to substract float to date', () => {
        // these should throw (#todo)
        assert.throws(() => many(`select CURRENT_DATE - 7.5`), /operator does not exist: date - float/);
        assert.throws(() => many(`select CURRENT_DATE - 7.5::float`), /operator does not exist: date - float/);
    })

});
