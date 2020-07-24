import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';
import { Types } from '../datatypes';

describe('[Queries] Updates', () => {

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
});