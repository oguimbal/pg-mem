import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { preventSeqScan, preventCataJoin, watchCataJoins } from './test-utils';
import { _IDb } from '../interfaces-private';

describe('Joins', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    function explainMapSelect() {
        const expl = db.public.explainLastSelect()!;
        if (expl._ !== 'map') {
            assert.fail('should be a map');
        }
        return expl.of;
    }

    it('simple join on index', () => {
        preventCataJoin(db);
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
        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'join',
            id: 2,
            inner: true,
            restrictive: { _: 'table', table: 'ta' },
            joined: { _: 'table', table: 'tb' },
            on: {
                iterate: 'ta',
                iterateSide: 'restrictive',
                joinIndex: {
                    _: 'btree',
                    btree: ['bid'],
                    onTable: 'tb',
                },
                matches: { on: 'ta', col: 'bid' },
            }
        })
    });


    it('seq-scan inner join', () => {
        const result = many(`create table ta(aid text, bid text);
                            create table tb(bid text, val text);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values ('bid1', 'val1');
                            insert into tb values ('bid2', 'val2');

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid`);

        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'join',
            id: 2,
            inner: true,
            restrictive: { _: 'table', table: 'ta' },
            joined: { _: 'table', table: 'tb' },
            on: {
                seqScan: {
                    col: 'bid = bid',
                    on: 2,// <== directly on join
                } as any,
            }
        })

        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
        ]);
    });

    it('reverses inner join on index when lots of left values and index present', () => {
        preventCataJoin(db);
        const result = many(`create table ta(aid text primary key, bid text);
                            create table tb(bid text primary key, val text);
                            create index on ta(bid);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');
                            insert into ta values ('aid3', null);
                            insert into ta values ('aid4', null);

                            insert into tb values ('bid1', 'val1');
                            insert into tb values ('bid2', 'val2');

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid`);
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
        ]);
        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'join',
            id: 2,
            inner: true,
            restrictive: { _: 'table', table: 'ta' },
            joined: { _: 'table', table: 'tb' },
            on: {
                iterate: 'tb', // has inversed inner join
                iterateSide: 'joined',
                joinIndex: {
                    _: 'btree',
                    btree: ['bid'],
                    onTable: 'ta',
                },
                matches: { on: 'tb', col: 'bid' },
            }
        })
    });



    it('does not join on null values when using index', () => {
        preventCataJoin(db);
        const result = many(`create table ta(aid text primary key, bid text);
                            create table tb(bid text, val text);
                            create index on tb(bid);
                            insert into ta values ('aid1', null);
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values (null, 'val1');
                            insert into tb values ('bid2', 'val2');

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid`);
        expect(result).to.deep.equal([
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
        ]);
        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'join',
            id: 2,
            inner: true,
            restrictive: { _: 'table', table: 'ta' },
            joined: { _: 'table', table: 'tb' },
            on: {
                iterate: 'ta',
                iterateSide: 'restrictive',
                joinIndex: {
                    _: 'btree',
                    btree: ['bid'],
                    onTable: 'tb',
                },
                matches: { on: 'ta', col: 'bid' },
            }
        })
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
        preventCataJoin(db);
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
        preventCataJoin(db);
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

        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'join',
            id: 2,
            restrictive: { _: 'table', table: 'ta' },
            joined: { _: 'table', table: 'tb' },
            inner: false,
            on: {
                iterate: 'ta',
                iterateSide: 'restrictive',
                joinIndex: {
                    _: 'btree',
                    btree: ['bid'],
                    onTable: 'tb',
                },
                matches: {
                    on: 'ta',
                    col: 'bid',
                }
            }
        });
    });



    it('right outer join with left null values', () => {
        const watch = watchCataJoins(db);
        const result = many(`create table ta(aid text primary key, bida text);
                            create table tb(bid text primary key, val text);
                            create index on ta(bida);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values ('bid1', 'val1');

                            select val, aid, ta.bida as abid, tb.bid as bbid from ta
                                right outer join tb on ta.bida = tb.bid`);

        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'join',
            inner: false,
            id: 2,
            restrictive: { _: 'table', table: 'tb' },
            joined: { _: 'table', table: 'ta' },
            on: {
                iterate: 'tb',
                iterateSide: 'restrictive',
                joinIndex: {
                    _: 'btree',
                    btree: ['bida'],
                    onTable: 'ta',
                },
                matches: { on: 'tb', col: 'bid' },
            },
        });
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' }
        ]);
        watch.check();
    });



    it('condition on left part of join', () => {
        preventCataJoin(db);
        const result = many(`create table ta(aid text primary key, bid text, num int);
                            create table tb(bid text primary key, val text, num int);
                            insert into ta values ('aid1', 'bid1', 42);
                            insert into ta values ('aid2', 'bid2', 51);

                            insert into tb values ('bid1', 'val1', 10);
                            insert into tb values ('bid2', 'val2', 12);

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid
                            where ta.num > 42`);

        expect(result).to.deep.equal([
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
        ]);
        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'seqFilter',
            id: 2,
            filtered: {
                _: 'join',
                inner: true,
                id: 3,
                restrictive: { _: 'table', table: 'ta' },
                joined: { _: 'table', table: 'tb' },
                on: {
                    iterate: 'ta',
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['bid'],
                        onTable: 'tb',
                    },
                    matches: { on: 'ta', col: 'bid' },
                },
            }
        });
    });



    it('condition on right part of join', () => {
        preventCataJoin(db);
        const result = many(`create table ta(aid text primary key, bid text, num int);
                            create table tb(bid text primary key, val text, num int);
                            insert into ta values ('aid1', 'bid1', 42);
                            insert into ta values ('aid2', 'bid2', 51);

                            insert into tb values ('bid1', 'val1', 10);
                            insert into tb values ('bid2', 'val2', 12);

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta
                                join tb on ta.bid = tb.bid
                            where tb.num < 12`);
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
        ]);
        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'seqFilter',
            id: 2,
            filtered: {
                _: 'join',
                inner: true,
                id: 3,
                restrictive: { _: 'table', table: 'ta' },
                joined: { _: 'table', table: 'tb' },
                on: {
                    iterate: 'ta',
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['bid'],
                        onTable: 'tb',
                    },
                    matches: { on: 'ta', col: 'bid' },
                },
            }
        });
    });



    it('OR condition on right both parts of join', () => {
        preventCataJoin(db);
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
        expect(result).to.deep.equal([
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2', anum: 100, bnum: 1 },
            { val: 'val1', aid: 'aid3', abid: 'bid1', bbid: 'bid1', anum: 1, bnum: 100 },
        ]);

        const expl = explainMapSelect();
        assert.deepEqual(expl, {
            _: 'seqFilter',
            id: 2,
            filtered: {
                _: 'join',
                inner: true,
                id: 3,
                restrictive: { _: 'table', table: 'ta' },
                joined: { _: 'table', table: 'tb' },
                on: {
                    iterate: 'ta',
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['bid'],
                        onTable: 'tb',
                    },
                    matches: { on: 'ta', col: 'bid' },
                },
            }
        });
    });


    it('AND condition on right both parts of join', () => {
        preventCataJoin(db);
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
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1', anum: 1, bnum: 1 },
        ]);
    });



    function photos() {
        none(`CREATE TABLE "user" ("id" text primary key, "name" text NOT NULL);
        CREATE TABLE "photo" ("id" text primary key, "url" text NOT NULL, "userId" text);
        create index on photo("userId");
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
        delete (sel as any)['select'];
        assert.deepEqual(sel, {
            _: 'map',
            id: 1,
            of: {
                _: 'join',
                inner: true,
                id: 2,
                restrictive: {
                    _: 'table',
                    table: 'user',
                },
                joined: {
                    _: 'table',
                    table: 'photo',
                },
                on: {
                    iterate: 'user',
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['userId'],
                        onTable: 'photo',
                    },
                    matches: {
                        on: 'user',
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
        delete (sel as any)['select'];
        assert.deepEqual(sel, {
            _: 'map',
            id: 1,
            of: {
                _: 'join',
                inner: true,
                id: 2,
                restrictive: {
                    _: 'table',
                    table: 'user',
                },
                joined: {
                    _: 'table',
                    table: 'photo',
                },
                on: {
                    iterate: 'user',
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['userId'],
                        onTable: 'photo',
                    },
                    matches: {
                        on: 'user',
                        col: 'id',
                    },
                    filtered: true,
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
        delete (sel as any)['select'];
        assert.deepEqual(sel, {
            _: 'map',
            id: 1,
            of: {
                _: 'join',
                inner: true,
                id: 2,
                restrictive: {
                    _: 'table',
                    table: 'user',
                },
                joined: {
                    _: 'table',
                    table: 'photo',
                },
                on: {
                    iterate: 'user',
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['userId'],
                        onTable: 'photo',
                    },
                    matches: {
                        on: 'user',
                        col: 'id',
                    },
                    filtered: true
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

    it('can self right join', () => {
        const got = many(`create table test(usr text, friend text);
        insert into test values ('me', 'you');
        insert into test values ('me', 'other1');
        insert into test values ('you', 'me');
        insert into test values ('you', 'other2');
        select a.usr, b.friend from
            test a
            right join test b on a.friend = b.usr;`);
        expect(got)
            .to.deep.equal([
                { usr: 'you', friend: 'you' }
                , { usr: 'you', friend: 'other1' }
                , { usr: 'me', friend: 'me' }
                , { usr: 'me', friend: 'other2' }
            ])
    })

    it('can self left join', () => {
        const got = many(`create table test(usr text, friend text);
        insert into test values ('me', 'you');
        insert into test values ('me', 'other1');
        insert into test values ('you', 'me');
        insert into test values ('you', 'other2');
        select a.usr, b.friend from
            test a
            left join test b on a.friend = b.usr;`);
        expect(got)
            .to.deep.equal([
                { usr: 'me', friend: 'me' },
                { usr: 'me', friend: 'other2' },
                { usr: 'me', friend: null },
                { usr: 'you', friend: 'you' },
                { usr: 'you', friend: 'other1' },
                { usr: 'you', friend: null },
            ])
    });

    it('can self inner join', () => {
        const got = many(`create table test(usr text, friend text);
        insert into test values ('me', 'you');
        insert into test values ('me', 'other1');
        insert into test values ('you', 'me');
        insert into test values ('you', 'other2');
        select a.usr, b.friend from
            test a
            join test b on a.friend = b.usr;`);
        expect(got)
            .to.deep.equal([
                { usr: 'me', friend: 'me' },
                { usr: 'me', friend: 'other2' },
                { usr: 'you', friend: 'you' },
                { usr: 'you', friend: 'other1' },
            ])
    });

    it('can select * and column on join', () => {
        expect(many(`select *, a from concat('a') as a join concat('a') as b on a.a=b.b`))
            .to.deep.equal([{ a: 'a', b: 'a' }]);
    })

    it('can select selective * and column on join ', () => {
        expect(many(`select b.*, vala, a from (values ('x', 'a1')) as a(ida, vala) join (values ('x', 'b1')) as b(idb, valb) on a.ida=b.idb`))
            .to.deep.equal([{
                idb: 'x',
                valb: 'b1',
                vala: 'a1',
                a: {
                    ida: 'x',
                    vala: 'a1',
                },
            }]);
    })

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
