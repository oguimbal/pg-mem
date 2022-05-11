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
        none(`create table people (name text, age int, loc text);
        insert into people values ('jess', 30, 'en'), ('kevin', 14, 'fr'), ('lea', 10, 'fr'), ('oliver', 34, 'en');`);
    }

    it('can create simple view', () => {
        people();
        expect(
            many(`create view minors as select name from people where age < 18;
            select * from minors`),
        ).to.deep.equal([{ name: 'kevin' }, { name: 'lea' }]);
    });

    it('can create view with column names', () => {
        people();
        expect(
            many(`create view minors(nm) as select name, age from people where age < 18;
            select * from minors`),
        ).to.deep.equal([
            { nm: 'kevin', age: 14 },
            { nm: 'lea', age: 10 },
        ]);
    });

    it('can update view with new rows', () => {
        people();
        expect(
            many(`create view minors(nm) as select name, age from people where age < 18;
              select * from minors`),
        ).to.deep.equal([
            { nm: 'kevin', age: 14 },
            { nm: 'lea', age: 10 },
        ]);

        none(`insert into people values ('victor', 3, 'en');`);

        expect(many(`select * from minors`)).to.deep.equal([
            { nm: 'kevin', age: 14 },
            { nm: 'lea', age: 10 },
            { nm: 'victor', age: 3 },
        ]);
    });
});
