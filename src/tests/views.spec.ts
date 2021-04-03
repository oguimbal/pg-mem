import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';

describe('Views', () => {

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

    function people() {
        none(`create table people (name text, age int);
        insert into people values ('jess', 30), ('kevin', 14), ('lea', 10), ('oliver', 34);`);
    }

    it('can create view', () => {
        people();
        expect(many(`create view minors as select name from people where age < 18;
            select * from minors`))
            .to.deep.equal([
                { name: 'kevin' },
                { name: 'lea' },
            ]);
    });
});
