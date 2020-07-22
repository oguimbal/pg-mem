import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('Typeorm', () => {

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
        db.query.none('create table data(id text primary key, data jsonb, num integer, var varchar(10))');
    }


    it('can process typeorm columns schema selection', () => {
        simpleDb();
        expect(many(`SELECT *, ('"' || "udt_schema" || '"."' || "udt_name" || '"')::"regtype" AS "regtype" FROM "information_schema"."columns" WHERE ("table_schema" = 'public' AND "table_name" = 'data')`).length)
            .to.equal(4);
        expect(many(`SELECT ('"' || "udt_schema" || '"."' || "udt_name" || '"')::"regtype" AS "regtype" FROM "information_schema"."columns" WHERE ("table_schema" = 'public' AND "table_name" = 'data')`))
            .to.deep.equal([{ regtype: 'text' }
                , { regtype: 'jsonb' }
                , { regtype: 'integer' }
                , { regtype: 'text' }]);
    });

    it ('can select table schema', () => {
        simpleDb();
        const result = many(`SELECT "ns"."nspname" AS "table_schema",
                "t"."relname" AS "table_name",
                "cnst"."conname" AS "constraint_name",
                pg_get_constraintdef("cnst"."oid") AS "expression",
                CASE "cnst"."contype"
                    WHEN 'p' THEN 'PRIMARY'
                    WHEN 'u' THEN 'UNIQUE'
                    WHEN 'c' THEN 'CHECK'
                    WHEN 'x' THEN 'EXCLUDE'
                END AS "constraint_type",
                "a"."attname" AS "column_name"
            FROM "pg_constraint" "cnst"
            INNER JOIN "pg_class" "t" ON "t"."oid" = "cnst"."conrelid"
            INNER JOIN "pg_namespace" "ns" ON "ns"."oid" = "cnst"."connamespace"
            LEFT JOIN "pg_attribute" "a" ON "a"."attrelid" = "cnst"."conrelid"
                                                AND "a"."attnum" = ANY ("cnst"."conkey")
            WHERE "t"."relkind" = 'r'
                    AND (("ns"."nspname" = 'public' AND "t"."relname" = 'data'))`);
            expect(result).to.deep.equal([])
    });

});