import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { typeormSimpleSample } from '../../samples/typeorm/simple';
import { typeormJoinsSample } from '../../samples/typeorm/joins';
import { _IDb } from '../interfaces-private';
import { Entity, BaseEntity, PrimaryColumn, PrimaryGeneratedColumn, Column, Connection } from 'typeorm';

describe('Typeorm', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb({
            autoCreateForeignKeyIndices: true,
        }) as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    function simpleDb() {
        db.public.none('create table data(id text primary key, data jsonb, num integer, var varchar(10))');
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

    it('can select table schema 1', () => {
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

    it('can select table schema 2', () => {
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


    it('can select table schema 3', () => {
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

    it('can select table schema 4', () => {
        const result = many(`SELECT * FROM "information_schema"."tables" WHERE "table_schema" = current_schema`);
        expect(result).to.deep.equal([])
    });


    it('can create table', () => {
        none(`CREATE TABLE "user" ("id" SERIAL NOT NULL,
                                    "firstName" text NOT NULL,
                                    "lastName" text NOT NULL,
                                    "age" integer NOT NULL,
                                    CONSTRAINT "PK_cace4a159ff9f2512dd42373760" PRIMARY KEY ("id")
                                );`);
        assert.exists(db.getTable('user'), 'Table should have been created');
    });


    function explainMapSelect() {
        const expl = db.public.explainLastSelect()!;
        if (expl._ !== 'map') {
            assert.fail('should be a map');
        }
        return expl.of;
    }

    it('can perform full join queries', () => {
        const got = many(`CREATE TABLE "user" ("id" SERIAL NOT NULL, "name" text NOT NULL, CONSTRAINT "PK_cace4a159ff9f2512dd42373760" PRIMARY KEY ("id"));
        CREATE TABLE "photo" ("id" SERIAL NOT NULL, "url" text NOT NULL, "userId" integer, CONSTRAINT "PK_723fa50bf70dcfd06fb5a44d4ff" PRIMARY KEY ("id"));
        ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87" FOREIGN KEY ("userId") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;
        INSERT INTO "photo"("url", "userId") VALUES ('photo-of-me-1.jpg', DEFAULT) RETURNING "id";
        INSERT INTO "photo"("url", "userId") VALUES ('photo-of-me-2.jpg', DEFAULT) RETURNING "id";
        INSERT INTO "user"("name") VALUES ('me') RETURNING "id";
        UPDATE "photo" SET "userId" = 1 WHERE "id" = 1;
        UPDATE "photo" SET "userId" = 1 WHERE "id" = 2;
        INSERT INTO "photo"("url", "userId") VALUES ('photo-of-you-1.jpg', DEFAULT) RETURNING "id";
        INSERT INTO "photo"("url", "userId") VALUES ('photo-of-you-2.jpg', DEFAULT) RETURNING "id";
        INSERT INTO "user"("name") VALUES ('you') RETURNING "id";
        UPDATE "photo" SET "userId" = 2 WHERE "id" = 3;
        UPDATE "photo" SET "userId" = 2 WHERE "id" = 4;
        SELECT "user"."id" AS "user_id", "user"."name" AS "user_name", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
            FROM "user" "user"
            LEFT JOIN "photo" "photo" ON "photo"."userId"="user"."id"
            WHERE "user"."name" = 'me';`);

        assert.deepEqual(explainMapSelect(), {
            _: 'seqFilter',
            id: 2,
            filtered: {
                _: 'join',
                id: 3,
                restrictive: { _: 'table', table: 'user' },
                joined: { _: 'table', table: 'photo' },
                inner: false,
                on: {
                    iterate: 'user',
                    iterateSide: 'restrictive',
                    joinIndex: {
                        _: 'btree',
                        btree: ['userId'],
                        onTable: 'photo'
                    },
                    matches: { on: 'user', col: 'id' },
                }
            }
        })

        expect(got).to.deep.equal([
            { user_id: 1, user_name: 'me', photo_id: 1, photo_url: 'photo-of-me-1.jpg', photo_userId: 1 },
            { user_id: 1, user_name: 'me', photo_id: 2, photo_url: 'photo-of-me-2.jpg', photo_userId: 1 },
        ])
    })

    async function typeOrm(entities: any[], fn: (db: Connection) => Promise<any>) {
        const got: Connection = await db.adapters.createTypeormConnection({
            type: 'postgres',
            entities,
        });
        try {
            await got.synchronize();
            await fn(got);
        } finally {
            await got.close()
        }
    }


    it('handles jsonb update', () => typeOrm([WithJsonb], async db => {
        const repo = db.getRepository(WithJsonb);
        const got = repo.create({
            data: [{ someData: true }]
        });
        await got.save();
        let all = await repo.findByIds([1]);
        expect(all.map(x => x.data)).to.deep.equal([[{ someData: true }]]);
        got.data = { other: true };
        await got.save();
        all = await repo.find();
        expect(all.map(x => x.data)).to.deep.equal([{other: true}]);
    }));

    it('can perform simple sample', async () => {
        await typeormSimpleSample();
    })

    it('can perform join sample', async () => {
        await typeormJoinsSample();
    });
});

@Entity()
class WithJsonb extends BaseEntity {
    @PrimaryGeneratedColumn({ type: 'integer' })
    id!: number;

    @Column({ type: 'jsonb' })
    data: any;
}