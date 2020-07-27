import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';
import { Types } from '../datatypes';
import { _IDb } from 'src/interfaces-private';

describe('[Queries] Selections', () => {

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
                constraint: { type: 'primary key' },
            }, {
                name: 'str',
                type: Types.text(),
            }, {
                name: 'otherStr',
                type: Types.text(),
            }],
        });
        return db;
    }

    function stuff() {
        none(`create table test(txt text, val integer);
        insert into test values ('A', 999);
        insert into test values ('A', 0);
        insert into test values ('A', 1);
        insert into test values ('B', 2);
        insert into test values ('C', 3);`)
    }


    it('can use transformations', () => {
        stuff();
        expect(many(`select * from (select val as xx from test where txt = 'A') x where x.xx >= 1`))
            .to.deep.equal([{ xx: 999 }, { xx: 1 }]);
    });

    it('executes the right plan on transformations', () => {
        stuff();
        const plan = (db as _IDb).public.explainSelect(`select * from (select val as valAlias from test where txt = 'A') x where x.valAlias >= 1`);
        // assert.deepEqual(plan, {} as any);
        assert.deepEqual(plan, {
            id: 1,
            type: 'seqFilter',
            filter: {
                id: 2,
                type: 'map',
                select: [{
                    what: {
                        col: 'val',
                        on: 3,
                    },
                    as: 'valAlias',
                }],
                of: {
                    id: 3,
                    type: 'seqFilter',
                    filter: {
                        id: 4,
                        type: 'table',
                        table: 'test',
                    }
                }
            }
        });
    })


    it('can use an expression on a transformed selection', () => {
        // preventSeqScan(db);
        expect(many(`select *, lower(txtx) as v from (select val as valx, txt as txtx from test where val >= 1) x where lower(x.txtx) = 'a'`))
            .to.deep.equal([{ txtx: 'A', valx: 999, v: 'a' }, { txtx: 'A', valx: 1, v: 'a' }]);
    });


    it('selects case whithout condition', () => {
        simpleDb();
        expect(many(`insert into data(id, str) values ('id1', 'SOME STRING'), ('id2', 'other string'), ('id3', 'Some String');
            select case when id='id1' then 'one ' || str else 'something else' end as x from data`))
            .to.deep.equal([{ x: 'one SOME STRING' }, { x: 'something else' }, { x: 'something else' }]);
    })

    it('selects case with disparate types results', () => {
        simpleDb();
        expect(many(`select case when 2 > 1 then 1.5 when 2 < 1 then 1 end as x`))
            .to.deep.equal([{ x: 1.5 }]);
    })

    it('does not support select * on dual', () => {
        assert.throw(() => many(`select *`));
    });

    it('supports concat operator', () => {
        expect(many(`select 'a' || 'b' as x`))
            .to.deep.equal([{ x: 'ab' }]);
    });

});