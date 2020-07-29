import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('[Queries] Count', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    function explainMapSelect() {
        const expl = db.public.explainLastSelect();
        if (expl._ !== 'map') {
            assert.fail('should be a map');
            return null;
        }
        return expl.of;
    }

    it('selects simple count', () => {
        expect(one(`create table test(val text);
            insert into test values ('a'), ('b'), (null);
            select count(*) as cnt from test`))
            .to.deep.equal({ cnt: 3 });
    });



    it('counts nulls the right way', () => {
        expect(one(`create table test(val int, val2 int);
        insert into test values (1, null), (null, 1), (1, 1), (2, 3), (null, null);
        select count(COALESCE(val,val2)) as a, COUNT(val+val2) as b, count(*) as c from test;`))
            .to.deep.equal({ a: 4, b: 2, c: 5 });
    });


    it('counts on group by the right way', () => {
        expect(many(`create table test(val int, val2 int);
                        insert into test values (1, null), (null, 1), (1, 1), (2, 3), (null, null);
                        select count(val2) as val2cnt, count(val) as valCnt,val from test group by val;`))
            .to.deep.equal([
                { val2cnt: 1, valCnt: 2, val: 1 }
                , { val2cnt: 1, valCnt: 0, val: null }
                , { val2cnt: 1, valCnt: 1, val: 2 }
            ])
    })

    it('can use index on expression to count', () => {
        preventSeqScan(db);
        expect(many(`create table test(a int, b int);
                        create index on test((a+b));
                        insert into test values (1, 1), (2, 0), (3, 0), (4, 0), (4, null);
                        select count(*) as cnt, b+a as g from test group by a+b;`))
            .to.deep.equal([
                { g: 2, cnt: 2 }
                , { g: 3, cnt: 1 }
                , { g: 4, cnt: 1 }
                , { g: null, cnt: 1 }
            ])
    })
});