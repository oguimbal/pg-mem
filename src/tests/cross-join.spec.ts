import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { preventSeqScan, preventCataJoin, watchCataJoins } from './test-utils';
import { _IDb } from '../interfaces-private';

describe('Cross/Carthesian joins', () => {

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

    function ab() {
        none(`create table ta (ida text, va int);
                create table tb (idb text, vb int);
                insert into ta values ('a1', 1);
                insert into ta values ('a2', 2);
                insert into tb values ('b1', 11);
                insert into tb values ('b2', 12);`)
    }

    it('simple cross join without condition', () => {
        ab();
        expect(many(`select * from  ta, tb`)).to.deep.equal([
            { ida: 'a1', va: 1, idb: 'b1', vb: 11 },
            { ida: 'a1', va: 1, idb: 'b2', vb: 12 },
            { ida: 'a2', va: 2, idb: 'b1', vb: 11 },
            { ida: 'a2', va: 2, idb: 'b2', vb: 12 }
        ]);
    })


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



    it('simple join on index with comma syntax', () => {
        const result = many(`create table ta(aid text primary key, bid text);
                            create table tb(bid text primary key, val text);
                            insert into ta values ('aid1', 'bid1');
                            insert into ta values ('aid2', 'bid2');

                            insert into tb values ('bid1', 'val1');
                            insert into tb values ('bid2', 'val2');

                            select val, aid, ta.bid as abid, tb.bid as bbid from ta, tb
                                where ta.bid = tb.bid`);
        preventSeqScan(db);
        expect(result).to.deep.equal([
            { val: 'val1', aid: 'aid1', abid: 'bid1', bbid: 'bid1' },
            { val: 'val2', aid: 'aid2', abid: 'bid2', bbid: 'bid2' },
        ]);
    });
});
