import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';

describe('Foreign keys', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    function usr(onDelete = 'NO ACTION') {
        none(`CREATE TABLE "user" ("id" SERIAL NOT NULL, "name" text NOT NULL, CONSTRAINT "PK_cace4a159ff9f2512dd42373760" PRIMARY KEY ("id"));
        CREATE TABLE "photo" ("id" SERIAL NOT NULL, "url" text NOT NULL, "userId" integer, CONSTRAINT "PK_723fa50bf70dcfd06fb5a44d4ff" PRIMARY KEY ("id"));
        ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87" FOREIGN KEY ("userId") REFERENCES "user"("id") ON DELETE ${onDelete} ON UPDATE NO ACTION;
        INSERT INTO "user"("id","name") VALUES (1, 'me');
        INSERT INTO "photo"("id", "url", "userId") VALUES (1, 'me-1.jpg', 1);`);
    }

    const violates = /update or delete on table "user" violates foreign key constraint/
    const locked = /cannot truncate a table referenced in a foreign key constraint/;

    it('cannot delete line if foreign key exists', () => {
        usr('NO ACTION');

        assert.throws(() => none('delete from "user" where id = 1'), violates);

        // check works if user is deleted
        many('truncate photo; delete from "user" where id = 1;')
    });



    it('cannot truncate if foreign key exists when no action', () => {
        usr('NO ACTION');
        assert.throws(() => none('truncate "user"'), locked);
    });

    it('cannot truncate if foreign key exists even if cascade', () => {
        usr('CASCADE');
        assert.throws(() => none('truncate "user"'), locked);
    });


    it('cannot drop if foreign key exists even if cascade', () => {
        usr('CASCADE');
        assert.throws(() => none('drop table "user"'), /cannot drop table "user" because other objects depend on it/);
    });


    it('can drop cascade', () => {
        usr('CASCADE');
        none('drop table "user" cascade');
        // constraint should have been droped as well:
        none(`INSERT INTO "photo"("id", "url", "userId") VALUES (2, 'me-1.jpg', 42);`)
    });

    it('can drop if foreign table dropped first', () => {
        usr('CASCADE');
        none(`drop table "photo";
                drop table "user";`);
    });

    it('can delete line when on delete cascade', () => {
        usr('CASCADE');

        expect(many('select * from photo').length).to.equal(1);

        none(`delete from "user" where id = 1;`);

        expect(many('select * from "user"').length).to.equal(0);
        expect(many('select * from photo').length).to.equal(0);
    });


    it('cannot create a foreign key if no unique constraint', () => {
        none(`CREATE TABLE "user" ("id" integer primary key, "name" text);
        CREATE TABLE "photo" ("id" integer primary key, "userName" text);`)
        assert.throws(() => none(`ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87" FOREIGN KEY ("userName") REFERENCES "user"("name") ON DELETE NO ACTION ON UPDATE NO ACTION;`))
    })

    it('does not check foreign key on null values', () => {
        none(`CREATE TABLE "user" ("id" integer primary key, "name" text unique);
        CREATE TABLE "photo" ("id" integer primary key, "userName" text);
        ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87" FOREIGN KEY ("userName") REFERENCES "user"("name") ON DELETE NO ACTION ON UPDATE NO ACTION;
        INSERT INTO "user"("id","name") VALUES (1, null);
        INSERT INTO "user"("id","name") VALUES (2, 'me');
        INSERT INTO "photo"("id", "userName") VALUES (1, null);
        INSERT INTO "photo"("id", "userName") VALUES (2, 'me');`);

        // check throws on matching username
        assert.throws(() => none('delete from "user" where id = 2;'));

        // check no throw then they are null
        none('delete from "user" where id = 1');
    });


    it('prevents updating foreign key', () => {
        none(`CREATE TABLE "user" ("id" integer primary key, "name" text unique);
        CREATE TABLE "photo" ("id" integer primary key, "userName" text);
        ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87" FOREIGN KEY ("userName") REFERENCES "user"("name") ON DELETE NO ACTION ON UPDATE NO ACTION;
        INSERT INTO "user"("id","name") VALUES (1, null);
        INSERT INTO "user"("id","name") VALUES (2, 'me');
        INSERT INTO "photo"("id", "userName") VALUES (1, null);
        INSERT INTO "photo"("id", "userName") VALUES (2, 'me');`);

        // check throws on matching username
        assert.throws(() => none(`update "user" set name='other' where id=2;`));

        // check OK on no-match values
        none(`update "user" set name='other' where id=1;`)
    })


    it('cannot insert wrong foreign key', () => {
        none(`CREATE TABLE "user" ("id" integer primary key, "name" text unique);
        CREATE TABLE "photo" ("id" integer primary key, "userName" text);
        ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87" FOREIGN KEY ("userName") REFERENCES "user"("name") ON DELETE NO ACTION ON UPDATE NO ACTION;`);

        // check throws when forein key is NOK
        assert.throws(() => none(`INSERT INTO "photo"("id", "userName") VALUES (2, 'blah');`));
    })



    it('can alter table add foreign key', () => {
        none(`create table test(t bool);
        alter table test add constraint "testkey" primary key (t);`);
    })



    it('can add a "match full" reference', () => {
        // https://github.com/oguimbal/pg-mem/issues/9
        none(`create table location(city_id int);
                create table city(city_id int primary key);
                ALTER TABLE ONLY public.location
                    ADD CONSTRAINT city_id_fk FOREIGN KEY (city_id) REFERENCES public.city(city_id) MATCH FULL;`);
    });



    it('cannot create a foreign key when no unique constraint on foreign table', () => {
        // https://github.com/oguimbal/pg-mem/issues/9
        none(`create table location(city_id int);
                create table city(city_id int);`);
        assert.throws(() => none(`ALTER TABLE ONLY public.location ADD CONSTRAINT city_id_fk FOREIGN KEY (city_id) REFERENCES public.city(city_id) MATCH FULL;`)
            , /there is no unique constraint matching given keys for referenced table "city"/)
    });


    it('can create foreign key with self-reference directly in table declaration', () => {
        none(`CREATE TABLE my_table (
                id text NOT NULL PRIMARY KEY,
                field1 text,
                field2 text NOT NULL,
                parent_id text,
                CONSTRAINT fk_id FOREIGN KEY (parent_id) REFERENCES my_table (id)
            )`);
    })

});