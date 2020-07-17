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

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid`);

        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
        ]);
    });



    it('join with left null values', () => {
        const result = many(`create table ta(aid text primary key, bid text);
                            create table tb(bid text primary key, val text);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values ('bid1', 'val1');

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid`);

        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' }
        ]);
    });


    it('left outer join with left null values', () => {
        const result = many(`create table ta(aid text primary key, bid text);
                            create table tb(bid text primary key, val text);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values ('bid1', 'val1');

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                left outer join tb on ta.bid = tb.bid`);

        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
            { val: null, aid: 'aid2', abid: 'bid2', bbid: null }
        ]);
    });



    it('right outer join with left null values', () => {
        const result = many(`create table ta(aid text primary key, bid text);
                            create table tb(bid text primary key, val text);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values ('bid1', 'val1');

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                right outer join tb on ta.bid = tb.bid`);

        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' }
        ]);
    });
});
