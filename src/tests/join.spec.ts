import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('Joins', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.query.many.bind(db.query);
        none = db.query.none.bind(db.query);
    });

    it('simple join on index', () => {
        const result = many(`create table ta(aid text primary key, bid text);
                            create table tb(bid text primary key, val text);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values ('bid1', 'val1');
                            insert into tb values ('bid2', 'val2');

                            select val from ta
                                join tb on ta.bid = tb.bid`);

        expect(result.map(x => x.val)).to.deep.equal(['val1', 'val2']);
    });
});
