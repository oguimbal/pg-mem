import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { Types } from '../datatypes';
import { preventCataJoin } from './test-utils';

describe('Updates', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    function simpleDb() {
        db.public.declareTable({
            name: 'data',
            fields: [{
                name: 'id',
                type: Types.text(),
                constraints: [{ type: 'primary key' }],
            }, {
                name: 'str',
                type: Types.text(),
            }, {
                name: 'otherstr',
                type: Types.text(),
            }],
        });
        return db;
    }



    it('rollbacks in case of update failure', () => {
        assert.throws(() => none(`create table test(key text, val integer unique);
                    insert into test values ('a', 1), ('x', 2), ('a', 3);
                    commit;
                    update test set val = 1 where key = 'a';`));
        expect(many(`select * from test`))
            .to.deep.equal([{ key: 'a', val: 1 }, { key: 'x', val: 2 }, { key: 'a', val: 3 }])
    });

    it('rollbacks all in case of update failure', () => {
        assert.throws(() => none(`create table test(key text, val integer unique);
                    insert into test values ('a', 1), ('x', 2), ('a', 3);
                    commit;
                    update test set val = 3 where key = 'a';`));
        expect(many(`select * from test`))
            .to.deep.equal([{ key: 'a', val: 1 }, { key: 'x', val: 2 }, { key: 'a', val: 3 }])
    });

    it('works if update matches constraint because same element', () => {
        none(`create table test(key text, val integer unique);
                    insert into test values ('a', 1), ('x', 2), ('a', 3);
                    commit;
                    update test set val = 2 where key = 'x';`);
        expect(many(`select * from test`))
            .to.deep.equal([{ key: 'a', val: 1 }, { key: 'a', val: 3 }, { key: 'x', val: 2 }])
    });


    it('can update', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        expect(many(`update data set str='something new';
                     select str from data;`))
            .to.deep.equal([{ str: 'something new' }]);
    })

    it('can update multiple', () => {
        expect(many(`create table test(key text, val integer);
                    insert into test values ('a', 1), ('x', 2), ('a', 3);
                    update test set val = 42 where key = 'a';
                    select * from test`))
            .to.deep.equal([{ key: 'a', val: 42 }, { key: 'x', val: 2 }, { key: 'a', val: 42 }])
    });

    it('can handle jsonb update', () => {
        expect(many(`create table test(val jsonb);
                    insert into test values ('{"data": true}');
                    update test set val = '{"other": true}';
                    select * from test`))
            .to.deep.equal([{ val: { other: true } }])
    });

    it('can handle bool update', () => {
        expect(many(`create table test(val boolean);
                    insert into test values ('Y');
                    update test set val = 'N';
                    select * from test`))
            .to.deep.equal([{ val: false }])
    });

    it('can use count in update subquery', () => {
        expect(many(`
            create table users(id text, cnt integer, books jsonb);
            create table books(user_id text, title text);
            insert into users values ('a', 0), ('b', 0);
            insert into books values ('a', 'ba'), ('b', 'bb'), ('b', 'bc'), ('b', 'bc');
            update users  set
                 books = (select jsonb_agg(distinct title) from books where books.user_id = 'b'),
                 cnt = (select count(*) from books where books.user_id = 'b')
            where id= 'b';
            select * from users where id='b';
        `)).to.deep.equal([{ id: 'b', cnt: 3, books: ['bb', 'bc'] }]);
    });


    it('can update from select', () => {
        preventCataJoin(db); // must use index
        expect(many(`
            create table test_table(id text primary key, value int);
            insert into test_table values ('a', 1), ('b', 2), ('c', 3);

            update test_table
                set value = value +  ids.val
            from (select * from (values ('b', 42)) ids(oid, val)) as ids
            where test_table.id = ids.oid;

        select * from test_table order by value;
            `)).to.deep.equal([
            { id: 'a', value: 1 },
            { id: 'c', value: 3 },
            { id: 'b', value: 2 + 42 },
        ]);
    });


    it('does nothing when update from select has no match', () => {
        preventCataJoin(db); // must use index
        expect(many(`
            create table test_table(id text primary key, value int);
            insert into test_table values ('a', 1), ('b', 2), ('c', 3);

            update test_table
                set value = 42
            from (select * from (values ('x')) ids(id)) as ids
            where test_table.id = ids.id;

        select * from test_table order by value;
            `)).to.deep.equal([
            { id: 'a', value: 1 },
            { id: 'b', value: 2 },
            { id: 'c', value: 3 },
        ]);
    });

    it('cannot update multiple times the same column in one query', () => {
        none(`create table test_table(id text primary key, value int);
        insert into test_table values ('a', 1), ('b', 2), ('c', 3);`)
        expect(() => many(`
            update test_table set value = 42, value = 43;
        `)).to.throw();
    });
});