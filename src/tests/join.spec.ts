import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';
import { _IDb } from '../interfaces-private';

describe('[Queries] Joins', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
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



    function photos() {
        none(`CREATE TABLE "user" ("id" text primary key, "name" text NOT NULL);
        CREATE TABLE "photo" ("id" text primary key, "url" text NOT NULL, "userId" text);
        create index on photo(userId);
        INSERT INTO "photo" VALUES ('p1', 'me-1.jpg', 'u1');
        INSERT INTO "photo" VALUES ('p2', 'me-2.jpg', 'u1');
        INSERT INTO "photo" VALUES ('p3', 'you-1.jpg', 'u2');
        INSERT INTO "photo" VALUES ('p0', 'noone.jpg', null);
        INSERT INTO "photo" VALUES ('p4', 'you-2.jpg', 'u2');
        INSERT INTO "photo" VALUES ('p5', 'somebody.jpg', 'x');
        INSERT INTO "user" VALUES ('u1', 'me');
        INSERT INTO "user" VALUES ('u2', 'you');
        INSERT INTO "user" VALUES ('u3', 'no camera');
        `)
    }


    it('can inner join', () => {
        photos();
        preventSeqScan(db, 'photo');
        const query = `SELECT "user"."id" AS "user_id", "user"."name" AS "user_name", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
            FROM "user" "user"
            JOIN "photo" "photo" ON "photo"."userId"="user"."id"`;
        const sel = db.public.explainSelect(query);
        delete sel['select'];
        assert.deepEqual(sel, {
            _: 'map',
            id: 1,
            of: {
                _: 'join',
                inner: true,
                id: 2,
                restrictive: {
                    _: 'table',
                    id: 3,
                    table: 'user',
                },
                joined: {
                    _: 'table',
                    id: 4,
                    table: 'photo',
                },
                on: {
                    iterate: 3,
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['userId'],
                        onTable: 'photo',
                    },
                    matches: {
                        on: 3,
                        col: 'id',
                    }
                }
            }
        })
        const result = many(query);
        expect(result)
            .to.deep.equal([
                { user_id: 'u1', user_name: 'me', photo_id: 'p1', photo_url: 'me-1.jpg', photo_userId: 'u1' },
                { user_id: 'u1', user_name: 'me', photo_id: 'p2', photo_url: 'me-2.jpg', photo_userId: 'u1' },
                { user_id: 'u2', user_name: 'you', photo_id: 'p3', photo_url: 'you-1.jpg', photo_userId: 'u2' },
                { user_id: 'u2', user_name: 'you', photo_id: 'p4', photo_url: 'you-2.jpg', photo_userId: 'u2' },
            ]);
    });

    it('can inner join and filter on right', () => {
        photos();
        preventSeqScan(db, 'photo');
        const query = `SELECT "user"."id" AS "user_id", "user"."name" AS "user_name", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
            FROM "user" "user"
            JOIN "photo" "photo" ON "photo"."userId"="user"."id" AND "photo"."url" like 'you%'`;
        const sel = db.public.explainSelect(query);
        delete sel['select'];
        assert.deepEqual(sel, {
            _: 'map',
            id: 1,
            of: {
                _: 'join',
                inner: true,
                id: 2,
                restrictive: {
                    _: 'table',
                    id: 3,
                    table: 'user',
                },
                joined: {
                    _: 'table',
                    id: 4,
                    table: 'photo',
                },
                on: {
                    iterate: 3,
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['userId'],
                        onTable: 'photo',
                    },
                    matches: {
                        on: 3,
                        col: 'id',
                    }
                }
            }
        })
        const result = many(query);
        expect(result)
            .to.deep.equal([
                { user_id: 'u2', user_name: 'you', photo_id: 'p3', photo_url: 'you-1.jpg', photo_userId: 'u2' },
                { user_id: 'u2', user_name: 'you', photo_id: 'p4', photo_url: 'you-2.jpg', photo_userId: 'u2' },
            ]);
    });

    it('can inner join and filter on left', () => {
        photos();
        preventSeqScan(db, 'photo');
        const query = `SELECT "user"."id" AS "user_id", "user"."name" AS "user_name", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
            FROM "user" "user"
            JOIN "photo" "photo" ON "photo"."userId"="user"."id" AND "user"."name" like 'yo%'`;
        const sel = db.public.explainSelect(query);
        delete sel['select'];
        assert.deepEqual(sel, {
            _: 'map',
            id: 1,
            of: {
                _: 'join',
                inner: true,
                id: 2,
                restrictive: {
                    _: 'table',
                    id: 3,
                    table: 'user',
                },
                joined: {
                    _: 'table',
                    id: 4,
                    table: 'photo',
                },
                on: {
                    iterate: 3,
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['userId'],
                        onTable: 'photo',
                    },
                    matches: {
                        on: 3,
                        col: 'id',
                    }
                }
            }
        })
        const result = many(query);
        expect(result)
            .to.deep.equal([
                { user_id: 'u2', user_name: 'you', photo_id: 'p3', photo_url: 'you-1.jpg', photo_userId: 'u2' },
                { user_id: 'u2', user_name: 'you', photo_id: 'p4', photo_url: 'you-2.jpg', photo_userId: 'u2' },
            ]);
    });

    it('can left join', () => {
        photos();
        preventSeqScan(db, 'photo');
        const result = many(`SELECT "user"."id" AS "user_id", "user"."name" AS "user_name", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
                            FROM "user" "user"
                            LEFT JOIN "photo" "photo" ON "photo"."userId"="user"."id"`);
        expect(result)
            .to.deep.equal([
                { user_id: 'u1', user_name: 'me', photo_id: 'p1', photo_url: 'me-1.jpg', photo_userId: 'u1' },
                { user_id: 'u1', user_name: 'me', photo_id: 'p2', photo_url: 'me-2.jpg', photo_userId: 'u1' },
                { user_id: 'u2', user_name: 'you', photo_id: 'p3', photo_url: 'you-1.jpg', photo_userId: 'u2' },
                { user_id: 'u2', user_name: 'you', photo_id: 'p4', photo_url: 'you-2.jpg', photo_userId: 'u2' },
                { user_id: 'u3', user_name: 'no camera', photo_id: null, photo_url: null, photo_userId: null },
            ]);
    });


    it('can right join', () => {
        photos();
        preventSeqScan(db, 'user');
        const result = many(`SELECT "user"."id" AS "user_id", "user"."name" AS "user_name", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
                            FROM "user" "user"
                            RIGHT JOIN "photo" "photo" ON "photo"."userId"="user"."id"`);
        expect(result)
            .to.deep.equal([
                { user_id: 'u1', user_name: 'me', photo_id: 'p1', photo_url: 'me-1.jpg', photo_userId: 'u1' },
                { user_id: 'u1', user_name: 'me', photo_id: 'p2', photo_url: 'me-2.jpg', photo_userId: 'u1' },
                { user_id: 'u2', user_name: 'you', photo_id: 'p3', photo_url: 'you-1.jpg', photo_userId: 'u2' },
                { user_id: null, user_name: null, photo_id: 'p0', photo_url: 'noone.jpg', photo_userId: null },
                { user_id: 'u2', user_name: 'you', photo_id: 'p4', photo_url: 'you-2.jpg', photo_userId: 'u2' },
                { user_id: null, user_name: null, photo_id: 'p5', photo_url: 'somebody.jpg', photo_userId: 'x' },
            ]);
    });

    // it ('can full join', () => {
    //     photos();
    //     const result = many(`SELECT "user"."id" AS "user_id", "user"."name" AS "user_name", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
    //                         FROM "user" "user"
    //                         FULL JOIN "photo" "photo" ON "photo"."userId"="user"."id"`);
    //     expect(result)
    //         .to.deep.equal([
    //             {user_id: 'u1', user_name: 'me', photo_id: 'p1', photo_url: 'me-1.jpg', photo_userId: 'u1' },
    //             {user_id: 'u1', user_name: 'me', photo_id: 'p2', photo_url: 'me-2.jpg', photo_userId: 'u1' },
    //             {user_id: 'u2', user_name: 'you', photo_id: 'p3', photo_url: 'you-1.jpg', photo_userId: 'u2' },
    //             {user_id: 'u2', user_name: 'you', photo_id: 'p4', photo_url: 'you-2.jpg', photo_userId: 'u2' },
    //             {user_id: null, user_name: null, photo_id: 'p5', photo_url: 'somebody.jpg', photo_userId: 'x' },
    //             {user_id: null, user_name: null, photo_id: 'p6', photo_url: 'noone.jpg', photo_userId: null },
    //             {user_id: 'u3', user_name: 'no camera', photo_id: null, photo_url: null, photo_userId: null },
    //         ]);
    // });
});
