import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('Group-by', () => {

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

    it('supports ordering with aggregated queries', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 1), (1, 1), (3, 3), (3, 3), (2, 2), (2, 2);
                    select a, max(b) as b from example group by a order by a ASC`))
            .to.deep.equal([{ a: 1, b: 1 }, { a: 2, b: 2 }, { a: 3, b: 3 }]);
    });

    it('supports select from an aggregation', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 1), (1, 1), (3, 3), (3, 3), (2, 2), (2, 2);
                    select a+b as ab from (select a, max(b) as b from example group by a order by a ASC) t`))
            .to.deep.equal([{ ab: 2 }, { ab: 4 }, { ab: 6 }]);
    });

});