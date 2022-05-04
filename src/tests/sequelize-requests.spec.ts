import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';

describe.skip('Sequelize - requests', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb({
            autoCreateForeignKeyIndices: true,
        }) as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });



    it('can perform join on readonly table', () => {
        // this used to throw
        many(`SELECT pg_range.rngtypid, pg_type.typname
                FROM pg_range
                LEFT OUTER JOIN pg_type ON pg_type.oid = pg_range.rngtypid
            `);
    });

    it('parses introspection range query', () => {
        // this query is emitted by sequelize to introspect the database (used to fail)
        many(`with ranges as (
            select
                pg_range.rngtypid, pg_type.typname as rngtypname, pg_type.typarray as rngtyparray, pg_range.rngsubtype
            from
                pg_range
            left outer join pg_type on
                pg_type.oid = pg_range.rngtypid
            )select
                pg_type.typname,
                pg_type.typtype,
                pg_type.oid,
                pg_type.typarray,
                ranges.rngtypname,
                ranges.rngtypid,
                ranges.rngtyparray
            from
                pg_type
            left outer join ranges on
                pg_type.oid = ranges.rngsubtype
            where
                (pg_type.typtype in('b', 'e'));`)
    });


    it('parses introspection cross-join query', () => {
        many(`
            select
                i.relname as name,
                ix.indisprimary as primary,
                ix.indisunique as unique,
                ix.indkey as indkey,
                array_agg(a.attnum) as column_indexes,
                array_agg(a.attname) as column_names,
                pg_get_indexdef(ix.indexrelid) as definition
            from
                pg_class t,
                pg_class i,
                pg_index ix,
                pg_attribute a
            where
                t.oid = ix.indrelid
                and i.oid = ix.indexrelid
                and a.attrelid = t.oid
                and t.relkind = 'r'
                and t.relname = 'Users'
            group by
                i.relname,
                ix.indexrelid,
                ix.indisprimary,
                ix.indisunique,
                ix.indkey
            order by
                i.relname
        `);
    });
});
