import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('Simple queries', () => {

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

    function simpleDb() {
        db.declareTable({
            name: 'data',
            fields: [{
                id: 'id',
                type: Types.text(),
                primary: true,
            }, {
                id: 'str',
                type: Types.text(),
            }, {
                id: 'otherStr',
                type: Types.text(),
            }],
        });
        return db;
    }

    it('where on primary', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id) values ('some value')`);
        let got = many(`select * from data where id='SOME ID'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where id='some value'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);

    });


    it('where constant true', () => {
        simpleDb();
        none(`insert into data(id) values ('some value')`);
        let got = many('select * from data where 1 = 1');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some value' }]);
    });

    it('where constant false', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id) values ('some value')`);
        let got = many('select * from data where 1 = 0');
        expect(trimNullish(got)).to.deep.equal([]);
    });

    it('where on other', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where str='none'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where str='some str'`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('can insert and select null', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', null)`);
        let got = many('select * from data where str is null');
        expect(trimNullish(got)).to.deep.equal([{ id: 'some id' }]);
        got = many('select * from data where str is not null');
        expect(got).to.deep.equal([]);
    });

    it('does not equate null values on seq scan', () => {
        simpleDb();
        none(`insert into data(id, str, otherStr) values ('id1', null, null)`);
        none(`insert into data(id, str, otherStr) values ('id2', 'A', 'A')`);
        none(`insert into data(id, str, otherStr) values ('id3', 'A', 'B')`);
        none(`insert into data(id, str, otherStr) values ('id4', null, 'B')`);
        none(`insert into data(id, str, otherStr) values ('id5', 'A', null)`);
        const got = many('select * from data where str = otherStr');
        expect(got).to.deep.equal([{ id: 'id2', str: 'A', otherStr: 'A' }]);
    });

    function setupNulls() {
        const db = simpleDb()
        db.getTable('data')
            .createIndex(['str']);
        none(`insert into data(id, str) values ('id1', null)`);
        none(`insert into data(id, str) values ('id2', 'notnull2')`);
        none(`insert into data(id, str) values ('id3', null)`);
        none(`insert into data(id, str) values ('id4', 'notnull4')`);
        return db;
    }

    it('uses indexes for null values', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = many('select * from data where str is null');
        expect(got).to.deep.equal([{ id: 'id1', str: null }, { id: 'id3', str: null }]);
    });


    it('uses indexes for not null values', () => {
        const db = setupNulls();
        preventSeqScan(db);
        const got = many('select * from data where str is not null');
        expect(got).to.deep.equal([{ id: 'id2', str: 'notnull2' }, { id: 'id4', str: 'notnull4' }]);
    });

    it('"IN" clause with constants and no index', () => {
        simpleDb();
        none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
        const got = many(`select * from data where str in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
    });

    it('"IN" clause with constants index', () => {
        simpleDb();
        db.getTable('data').createIndex(['str']);
        preventSeqScan(db);
        none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
        const got = many(`select * from data where str in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
    });

    it('"IN" clause with no constant', () => {
        simpleDb();
        none(`insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')`);
        const got = many(`select * from data where id in (str, otherStr)`);
        expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
    });

    it('"IN" clause with constant value', () => {
        simpleDb();
        none("insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')");
        const got = many(`select * from data where 'A' in (str, otherStr)`);
        expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
    });

    it('"NOT IN" clause with constants and no index', () => {
        simpleDb();
        none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
        const got = many(`select * from data where str not in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
    });

    it('"NOT IN" clause with constants index', () => {
        simpleDb();
        db.getTable('data').createIndex(['str']);
        preventSeqScan(db);
        none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
        const got = many(`select * from data where str not in ('str1', 'str3')`);
        expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
    });


    it('AND query', () => {
        simpleDb();
        preventSeqScan(db);
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where id='some id' AND str='other'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where id='some id' and str='some str'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('OR query', () => {
        simpleDb();
        none(`insert into data(id, str) values ('some id', 'some str')`);
        let got = many(`select * from data where id='other' OR str='other'`);
        expect(got).to.deep.equal([]);
        got = many(`select * from data where id='some id' OR str='other'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
        got = many(`select * from data where id='some id' or str='some str'`);
        expect(got).to.deep.equal([{ id: 'some id', str: 'some str' }]);
    });


    it('@> on value query', () => {
        const result = many(`create table test(id text primary key, data jsonb);
                                        insert into test values ('id1', '{"prop": "A","in":1}'), ('id2', '{"prop": "B","in":2}'), ('id4', '{"prop": "A","in":3}'), ('id5', null);
                                        select id from test where data @> '{"prop": "A"}';`);
        expect(result.map(x => x.id)).to.deep.equal(['id1', 'id4']);
    });


    it('call lower in select', () => {
        simpleDb();
        none(`insert into data(id) values ('SOME STRING')`);
        const result = many(`select lower(id) from data`);
        expect(result).to.deep.equal([{ column0: 'some string' }]);
    });

    it('call lower in condition', () => {
        simpleDb();
        none(`insert into data(id, str) values ('id1', 'SOME STRING'), ('id2', 'other string'), ('id3', 'Some String')`);
        const result = many(`select id from data where lower(str)='some string'`);
        expect(result.map(x => x.id)).to.deep.equal(['id1', 'id3']);
    });


    it('can select current_schema', () => {
        simpleDb();
        expect(many('select * from current_schema')).to.deep.equal([{ current_schema: 'public' }]);
    });


    it('can select info tables', () => {
        simpleDb();
        expect(many('select table_name from information_schema.tables')).to.deep.equal([{ table_name: 'data' }]);
    });


    it('can select info columns', () => {
        simpleDb();
        expect(many(`select column_name from information_schema.columns where table_name='data'`))
            .to.deep.equal([{ column_name: 'id' }
                , { column_name: 'str' }
                , { column_name: 'otherStr' }]);
    });

    it('can process typeorm columns schema selection', () => {
        simpleDb();
        expect(many(`SELECT *, ('"' || "udt_schema" || '"."' || "udt_name" || '"')::"regtype" AS "regtype" FROM "information_schema"."columns" WHERE ("table_schema" = 'public' AND "table_name" = 'data')`))
            .to.deep.equal([{}])
    })
});
