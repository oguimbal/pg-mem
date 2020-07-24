import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';

describe('Queries: Selections', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('can use transformations', () => {
        // preventSeqScan(db);
        expect(many(`create table test(txt text, val integer);
                insert into test values ('A', 999);
                insert into test values ('A', 0);
                insert into test values ('A', 1);
                insert into test values ('B', 2);
                insert into test values ('C', 3);
                select * from (select val as xx from test where txt = 'A') x where x.xx >= 1`))
            .to.deep.equal([{ xx: 999 }, { xx: 1 }]);
    });


    it('can use an expression on a transformed selection', () => {
        // preventSeqScan(db);
        expect(many(`create table test(txt text, val integer);
                insert into test values ('A', 999);
                insert into test values ('A', 0);
                insert into test values ('A', 1);
                insert into test values ('B', 2);
                insert into test values ('C', 3);
                select *, lower(txtx) as v from (select val as valx, txt as txtx from test where val >= 1) x where lower(x.txtx) = 'a'`))
            .to.deep.equal([{ txtx: 'A', valx: 999, v: 'a' }, { txtx: 'A', valx: 1, v: 'a' }]);
    });
});