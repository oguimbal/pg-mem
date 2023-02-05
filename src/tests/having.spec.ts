import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('Having', () => {
    
    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    it('select simple group by having condition', () => {
        expect(many(`create table test(name text);
            insert into test values ('alex'), ('alex'), ('ben'), ('charlie');
            select name from test group by name having count(name) > 1`))
            .to.deep.equal([{ name: "alex" }]);
    });
});
