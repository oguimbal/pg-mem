import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('Group-by', () => {

    // ================== ⚠️ NB: many "group by" tests are in the aggregations.spec.ts file.

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

    it('supports ordering with aggregated queries', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 1), (1, 1), (3, 3), (3, 3), (2, 2), (2, 2);
                    select a, max(b) as b from example group by a order by a ASC`))
            .to.deep.equal([{ a: 1, b: 1 }, { a: 2, b: 2 }, { a: 3, b: 3 }]);
    });

    it('supports select from an aggregation', () => {
        expect(many(`create table example(a int, b int);
                    insert into example values (1, 1), (1, 1), (3, 3), (3, 3), (2, 2), (2, 2);
                    select a+b as ab from (select a, max(b) as b from example group by a order by a ASC) t`))
            .to.deep.equal([{ ab: 2 }, { ab: 4 }, { ab: 6 }]);
    });


    it('does not seq-scan on count(*)', () => {

        preventSeqScan(db);

        expect(many(`create table example(a int, b int);
                    create index on example(a);
                    insert into example values (1, 1), (1, 1), (3, 3), (3, 3), (2, 2), (2, 2);
                    select a, count(*) cnt from example group by a order by a`))
            .to.deep.equal([{ a: 1, cnt: 2 }, { a: 2, cnt: 2 }, { a: 3, cnt: 2 }]);
    });

    it('can select from optimized aggregation', () => {

        preventSeqScan(db);

        expect(many(`create table example(a int, b int);
                    create index on example(a);
                    insert into example values (1, 1), (1, 1), (3, 3), (3, 3), (2, 2), (2, 2);
                    select a + cnt sum from (select a, count(*) cnt from example group by a order by a) t`))
            .to.deep.equal([{ sum: 3 }, { sum: 4 }, { sum: 5 }]);
    });


    it('can select from non grouped optimized aggregation', () => {

        preventSeqScan(db);

        expect(many(`create table example(a int, b int);
                    insert into example values (1, 1),  (2, 2);
                    select  cnt * 42 sum from (select count(*) cnt from example) t`))
            .to.deep.equal([{ sum: 84 }]);
    })


    describe('can group by alias', () => {
        // fix for https://github.com/oguimbal/pg-mem/issues/216

        beforeEach(() => none(`CREATE TABLE test(field int);
        INSERT INTO test values (3),(1),(2);`));


        it('case 1', () => expect(many(`SELECT field FROM test GROUP BY field ORDER BY FIELD`).map(x => x.field))
            .to.deep.equal([1, 2, 3]));

        // this used to throw
        it('case 2', () => expect(many(`SELECT field aliased FROM test GROUP BY aliased order by field`).map(x => x.aliased))
            .to.deep.equal([1, 2, 3]));

        it('case 3', () => expect(many(`SELECT field aliased FROM test GROUP BY aliased order by aliased`).map(x => x.aliased))
            .to.deep.equal([1, 2, 3]));

        it('case 4', () => expect(many(`SELECT -field aliased FROM test GROUP BY aliased order by aliased`).map(x => x.aliased))
            .to.deep.equal([-3, -2, -1]));

        it('case 5', () => expect(many(`SELECT -field aliased FROM test GROUP BY -field order by -field`).map(x => x.aliased))
            .to.deep.equal([-3, -2, -1]));
    })

    it('prefers non aliased group when ambiguous', () => {
        none(`CREATE TABLE test(field int);
                INSERT INTO test values (3),(1),(2);`);

        expect(many(`SELECT field aliased, (field > 2) field FROM test GROUP BY field order by aliased`))
            .to.deep.equal([
                { aliased: 1, field: false },
                { aliased: 2, field: false },
                { aliased: 3, field: true },
            ]);
    });

    it('allows group by on expression', () => {
        none(`CREATE TABLE test(field int);
                INSERT INTO test values (3),(1),(2);`);

        expect(many(`SELECT (field > 2) field FROM test GROUP BY field >2 order by field`))
            .to.deep.equal([
                { field: false },
                { field: true },
            ]);
    });
    it('can group on base field computation', () => {
        none(`CREATE TABLE test(field int);
            INSERT INTO test values (3),(1),(2);`);


        expect(many(`SELECT -field FROM test GROUP BY  -field ORDER BY -field`).map(x => x.field))
            .to.deep.equal([-3, -2, -1]);
    });


    // todo: fix this edge case
    it.skip('cannot order by on non grouped field', () => {
        none(`CREATE TABLE test(field int);
            INSERT INTO test values (3),(1),(2);`);

        assert.throws(() => many(`SELECT -field FROM test GROUP BY  -field ORDER BY field`), /must appear in the GROUP BY clause or be used in an aggregate function/);
    });

    // todo: fix this edge case
    it.skip('fails on column not in group by computation', () => {
        none(`CREATE TABLE test(field int);
            INSERT INTO test values (3),(1),(2);`);

        assert.throws(() => many(`SELECT field FROM test GROUP BY  -field`), / must appear in the GROUP BY clause or be used in an aggregate function/);
    });

    it('cannot group on aliased computation', () => {
        none(`CREATE TABLE test(field int);
            INSERT INTO test values (3),(1),(2);`);

        // group on alias is just a trick... you cannot use them in actual computations.
        assert.throws(() => many(`SELECT field aliased FROM test GROUP BY  -aliased`), /column "aliased" does not exist/);
    });
});
