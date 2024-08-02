import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

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
        ).toEqual([{ name: 'kevin' }, { name: 'lea' }]);
    });

    it('can create view with column names', () => {
        people();
        expect(
            many(`create view minors(nm) as select name, age from people where age < 18;
            select * from minors`),
        ).toEqual([
            { nm: 'kevin', age: 14 },
            { nm: 'lea', age: 10 },
        ]);
    });

    it('can update view with new rows', () => {
        people();
        expect(
            many(`create view minors(nm) as select name, age from people where age < 18;
              select * from minors`),
        ).toEqual([
            { nm: 'kevin', age: 14 },
            { nm: 'lea', age: 10 },
        ]);

        none(`insert into people values ('victor', 3, 'en');`);

        expect(many(`select * from minors`)).toEqual([
            { nm: 'kevin', age: 14 },
            { nm: 'lea', age: 10 },
            { nm: 'victor', age: 3 },
        ]);
    });

    it('can update view with updated rows', () => {
        people();
        expect(
            many(`create view minors(nm) as select name, age from people where age < 18;
            select * from minors`),
        ).toEqual([
            { nm: 'kevin', age: 14 },
            { nm: 'lea', age: 10 },
        ]);

        none(`update "people" SET "age" = 12 WHERE "name" = 'lea';`);

        expect(many(`select * from minors`)).toEqual([
            { nm: 'kevin', age: 14 },
            { nm: 'lea', age: 12 },
        ]);
    });
});
