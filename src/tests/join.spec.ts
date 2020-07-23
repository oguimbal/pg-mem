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
        preventSeqScan(db);
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
        ]);
    });

    // it('simple join on index with comma syntax', () => {
    //     const result = many(`create table ta(aid text primary key, bid text);
    //                         create table tb(bid text primary key, val text);
    //                         insert into ta values ('aid1', 'bid1');
    //                         insert into ta values ('aid2', 'bid2');

    //                         insert into tb values ('bid1', 'val1');
    //                         insert into tb values ('bid2', 'val2');

    //                         select val, aid, ta.bid as abid, tb.bid as bbid from ta, tb
    //                             where ta.bid = tb.bid`);
    //     preventSeqScan(db);
    //     expect(result).to.deep.equal([
    //         { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
    //         { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
    //     ]);
    // });



    it('join with left null values', () => {
        const result = many(`create table ta(aid text primary key, bid text);
                            create table tb(bid text primary key, val text);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values ('bid1', 'val1');

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid`);

        preventSeqScan(db);
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

        preventSeqScan(db);
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

        preventSeqScan(db);
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' }
        ]);
    });



    it('condition on left part of join', () => {
        const result = many(`create table ta(aid text primary key, bid text, num int);
                            create table tb(bid text primary key, val text, num int);
                            insert into ta values ('aid1', 'bid1', 42);
                            insert into ta values ('aid2', 'bid2', 51);

                            insert into tb values ('bid1', 'val1', 10);
                            insert into tb values ('bid2', 'val2', 12);

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid
                            where ta.num > 42`);
        preventSeqScan(db);
        expect(result).to.deep.equal([
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
        ]);
    });



    it('condition on right part of join', () => {
        const result = many(`create table ta(aid text primary key, bid text, num int);
                            create table tb(bid text primary key, val text, num int);
                            insert into ta values ('aid1', 'bid1', 42);
                            insert into ta values ('aid2', 'bid2', 51);

                            insert into tb values ('bid1', 'val1', 10);
                            insert into tb values ('bid2', 'val2', 12);

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid
                            where tb.num < 12`);
        preventSeqScan(db);
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
        ]);
    });



    it('OR condition on right both parts of join', () => {
        const result = many(`create table ta(aid text primary key, bid text, num int);
                            create table tb(bid text primary key, val text, num int);
                            insert into ta values ('aid1', 'bid1', 100);
                            insert into ta values ('aid2', 'bid2', 100);
                            insert into ta values ('aid3', 'bid1', 1);

                            insert into tb values ('bid1', 'val1', 100);
                            insert into tb values ('bid2', 'val2', 1);

                            select val, aid, ta.bid as abid, tb.bid as bbid, ta.num as anum, tb.num as bnum
                             from ta
                                join tb on ta.bid = tb.bid
                            where ta.num < 10 OR tb.num < 10`);
        preventSeqScan(db);
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid3', abid: 'bid1', bbid: 'bid1', anum: 1, bnum: 100 },
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2', anum: 100, bnum: 1 },
        ]);
    });


    it('AND condition on right both parts of join', () => {
        const result = many(`create table ta(aid text primary key, bid text, num int);
                                create table tb(bid text primary key, val text, num int);
                                insert into ta values ('aid1', 'bid1', 1);
                                insert into ta values ('aid2', 'bid2', 100);
                                insert into ta values ('aid3', 'bid2', 1);

                                insert into tb values ('bid1', 'val1', 1);
                                insert into tb values ('bid2', 'val2', 100);

                                select val, aid, ta.bid as abid, tb.bid as bbid, ta.num as anum, tb.num as bnum
                                from ta
                                    join tb on ta.bid = tb.bid
                                where ta.num < 10 AND tb.num < 10`);
        preventSeqScan(db);
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1', anum: 1, bnum: 1 },
        ]);
    });


});
