import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';

describe('Various expressions', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    // https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
    describe('EXTRACT()', () => {
        it('extracts century', () => {
            expect(many(`SELECT EXTRACT(CENTURY FROM TIMESTAMP '2000-12-16 12:21:13') as v`))
                .to.deep.equal([{ v: 20 }]);
            expect(many(`SELECT EXTRACT(CENTURY FROM TIMESTAMP '2001-02-16 20:38:40') as v`))
                .to.deep.equal([{ v: 21 }]);
        })
    })
});