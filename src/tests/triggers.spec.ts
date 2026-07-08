import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('Triggers', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('BEFORE UPDATE can mutate NEW', () => {
        none(`create table t (id int, rev int);
               insert into t values (1, 0);
               create function bump() returns trigger as $$
                   begin new.rev = old.rev + 1; return new; end;
               $$ language plpgsql;
               create trigger t_bump before update on t for each row execute function bump();
               update t set id = id where id = 1;
               update t set id = id where id = 1`);
        expect(many(`select rev from t`)[0].rev).toEqual(2);
    });

    it('BEFORE INSERT can mutate NEW with IF/ELSE', () => {
        none(`create table t (id int, tag text);
               create function tag_it() returns trigger as $$
                   begin
                       if new.id > 5 then new.tag = 'big';
                       else new.tag = 'small';
                       end if;
                       return new;
                   end;
               $$ language plpgsql;
               create trigger t_tag before insert on t for each row execute function tag_it();
               insert into t(id) values (10), (2)`);
        expect(many(`select id, tag from t order by id`))
            .toEqual([{ id: 2, tag: 'small' }, { id: 10, tag: 'big' }]);
    });

    it('BEFORE INSERT returning NULL skips the row', () => {
        none(`create table t (id int);
               create function no_evens() returns trigger as $$
                   begin if new.id % 2 = 0 then return null; end if; return new; end;
               $$ language plpgsql;
               create trigger t_odd before insert on t for each row execute function no_evens();
               insert into t values (1), (2), (3), (4), (5)`);
        expect(many(`select id from t order by id`).map(r => r.id)).toEqual([1, 3, 5]);
    });

    it('BEFORE DELETE returning NULL blocks the delete', () => {
        none(`create table t (id int);
               insert into t values (1), (2);
               create function keep_one() returns trigger as $$
                   begin if old.id = 1 then return null; end if; return old; end;
               $$ language plpgsql;
               create trigger t_keep before delete on t for each row execute function keep_one();
               delete from t`);
        expect(many(`select id from t order by id`).map(r => r.id)).toEqual([1]);
    });

    it('AFTER INSERT trigger fires without altering the row', () => {
        none(`create table t (id int);
               create function noop() returns trigger as $$
                   begin return new; end;
               $$ language plpgsql;
               create trigger t_after after insert on t for each row execute function noop();
               insert into t values (1), (2)`);
        expect(many(`select id from t order by id`).map(r => r.id)).toEqual([1, 2]);
    });

    it('supports EXECUTE PROCEDURE syntax and OR-ed events', () => {
        none(`create table t (id int, seen int);
               create function mark() returns trigger as $$
                   begin new.seen = 1; return new; end;
               $$ language plpgsql;
               create trigger t_mark before insert or update on t
                   for each row execute procedure mark();
               insert into t(id) values (1)`);
        expect(many(`select seen from t`)[0].seen).toEqual(1);
    });

    it('drops a trigger by name', () => {
        none(`create table t (id int, seen int);
               create function mark() returns trigger as $$
                   begin new.seen = 1; return new; end;
               $$ language plpgsql;
               create trigger t_mark before insert on t for each row execute function mark();
               drop trigger t_mark on t;
               insert into t(id) values (1)`);
        expect(many(`select seen from t`)[0].seen ?? null).toEqual(null);
    });

    it('errors on a duplicate trigger name for the same table', () => {
        none(`create table t (id int);
               create function noop() returns trigger as $$ begin return new; end; $$ language plpgsql;
               create trigger dup before insert on t for each row execute function noop()`);
        expectQueryError(() => none(
            `create trigger dup before insert on t for each row execute function noop()`),
            /already exists/);
    });

    it('DROP TRIGGER IF EXISTS is a no-op when absent', () => {
        none(`create table t (id int);
               drop trigger if exists nope on t`);
    });
});
