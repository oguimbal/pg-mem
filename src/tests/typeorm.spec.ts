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

    it ('can select table schema 1', () => {
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

    it ('can select table schema 2', () => {
        simpleDb();
        const result = many(`SELECT "ns"."nspname" AS "table_schema",
                                         "t"."relname" AS "table_name",
                                         "i"."relname" AS "constraint_name",
                                         "a"."attname" AS "column_name",
                                         CASE "ix"."indisunique"
                                             WHEN 't' THEN 'TRUE'
                                             ELSE'FALSE'
                                        END AS "is_unique",
                                        pg_get_expr("ix"."indpred", "ix"."indrelid") AS "condition",
                                        "types"."typname" AS "type_name"
                                    FROM "pg_class" "t"
                                    INNER JOIN "pg_index" "ix" ON "ix"."indrelid" = "t"."oid"
                                    INNER JOIN "pg_attribute" "a" ON "a"."attrelid" = "t"."oid"  AND "a"."attnum" = ANY ("ix"."indkey")
                                    INNER JOIN "pg_namespace" "ns" ON "ns"."oid" = "t"."relnamespace"
                                    INNER JOIN "pg_class" "i" ON "i"."oid" = "ix"."indexrelid"
                                    INNER JOIN "pg_type" "types" ON "types"."oid" = "a"."atttypid"
                                    LEFT JOIN "pg_constraint" "cnst" ON "cnst"."conname" = "i"."relname"
                                    WHERE "t"."relkind" = 'r'
                                        AND "cnst"."contype" IS NULL
                                        AND (("ns"."nspname" = 'public' AND "t"."relname" = 'user'));`);
            expect(result).to.deep.equal([])
    });


    it ('can select table schema 3', () => {
        simpleDb();
        const result = many(`SELECT
                                    "con"."conname" AS "constraint_name",
                                    "con"."nspname" AS "table_schema",
                                    "con"."relname" AS "table_name",
                                    "att2"."attname" AS "column_name",
                                    "ns"."nspname" AS "referenced_table_schema",
                                    "cl"."relname" AS "referenced_table_name",
                                    "att"."attname" AS "referenced_column_name",
                                    "con"."confdeltype" AS "on_delete",
                                    "con"."confupdtype" AS "on_update",
                                    "con"."condeferrable" AS "deferrable",
                                    "con"."condeferred" AS "deferred"
                                FROM (
                                    SELECT
                                        UNNEST ("con1"."conkey") AS "parent",
                                        UNNEST ("con1"."confkey") AS "child",
                                        "con1"."confrelid",
                                        "con1"."conrelid",
                                        "con1"."conname",
                                        "con1"."contype",
                                        "ns"."nspname", "cl"."relname",
                                        "con1"."condeferrable",
                                        CASE
                                            WHEN "con1"."condeferred" THEN 'INITIALLY DEFERRED'
                                            ELSE 'INITIALLY IMMEDIATE'
                                        END as condeferred,
                                        CASE "con1"."confdeltype"
                                            WHEN 'a' THEN 'NO ACTION'
                                            WHEN 'r' THEN 'RESTRICT'
                                            WHEN 'c' THEN 'CASCADE'
                                            WHEN 'n' THEN 'SET NULL'
                                            WHEN 'd' THEN 'SET DEFAULT'
                                        END as "confdeltype",
                                        CASE "con1"."confupdtype"
                                            WHEN 'a' THEN 'NO ACTION'
                                            WHEN 'r' THEN 'RESTRICT'
                                            WHEN 'c' THEN 'CASCADE'
                                            WHEN 'n' THEN 'SET NULL'
                                            WHEN 'd' THEN 'SET DEFAULT'
                                        END as "confupdtype"
                                    FROM "pg_class" "cl"
                                    INNER JOIN "pg_namespace" "ns" ON "cl"."relnamespace" = "ns"."oid"
                                    INNER JOIN "pg_constraint" "con1" ON "con1"."conrelid" = "cl"."oid"
                                    WHERE "con1"."contype" = 'f' AND (("ns"."nspname" = 'public' AND "cl"."relname" = 'user'))
                                ) "con"
                                INNER JOIN "pg_attribute" "att" ON "att"."attrelid" = "con"."confrelid"
                                                                        AND "att"."attnum" = "con"."child"
                                INNER JOIN "pg_class" "cl" ON "cl"."oid" = "con"."confrelid"
                                INNER JOIN "pg_namespace" "ns" ON "cl"."relnamespace" = "ns"."oid"
                                INNER JOIN "pg_attribute" "att2" ON "att2"."attrelid" = "con"."conrelid" AND "att2"."attnum" = "con"."parent";`);
            expect(result).to.deep.equal([])
    });

    it ('can select table schema 4', () => {
        const result = many(`SELECT * FROM "information_schema"."tables" WHERE "table_schema" = current_schema`);
            expect(result).to.deep.equal([])
    });


    it ('can create table', () => {
        none(`CREATE TABLE "user" ("id" SERIAL NOT NULL,
                                    "firstName" text NOT NULL,
                                    "lastName" text NOT NULL,
                                    "age" integer NOT NULL,
                                    CONSTRAINT "PK_cace4a159ff9f2512dd42373760" PRIMARY KEY ("id")
                                );`);
        assert.exists(db.getTable('user'), 'Table should have been created');
    })
});