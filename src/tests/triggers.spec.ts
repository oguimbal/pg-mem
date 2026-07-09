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

    it('a WHEN condition gates row-trigger firing', () => {
        none(`create table t (id int, n int, tag text);
               insert into t values (1, 5, null), (2, 15, null);
               create function mark() returns trigger as $$
                   begin new.tag = 'big'; return new; end;
               $$ language plpgsql;
               create trigger tw before update on t for each row
                   when (new.n > 10) execute function mark();
               update t set n = n`);
        expect(many(`select id, tag from t order by id`))
            .toEqual([{ id: 1, tag: null }, { id: 2, tag: 'big' }]);
    });

    it('UPDATE OF fires only when a listed column changes', () => {
        none(`create table u (id int, a int, b int, hits int default 0);
               insert into u values (1, 1, 1, 0);
               create function bump() returns trigger as $$
                   begin new.hits = old.hits + 1; return new; end;
               $$ language plpgsql;
               create trigger tu before update of a on u for each row execute function bump()`);
        none(`update u set b = 99 where id = 1`); // b changed, not a
        expect(many(`select hits from u`)[0].hits).toEqual(0);
        none(`update u set a = 42 where id = 1`); // a changed
        expect(many(`select hits from u`)[0].hits).toEqual(1);
    });

    it('an audit trigger can INSERT into another table using NEW/OLD', () => {
        none(`create table accounts (id int, balance int);
               create table audit_log (account_id int, old_bal int, new_bal int);
               create function log_change() returns trigger as $$
                   begin
                       insert into audit_log(account_id, old_bal, new_bal)
                           values (new.id, old.balance, new.balance);
                       return new;
                   end;
               $$ language plpgsql;
               create trigger acc_audit after update on accounts
                   for each row execute function log_change();
               insert into accounts values (1, 100), (2, 200);
               update accounts set balance = 150 where id = 1;
               update accounts set balance = 250 where id = 2`);
        expect(many(`select account_id, old_bal, new_bal from audit_log order by account_id`))
            .toEqual([
                { account_id: 1, old_bal: 100, new_bal: 150 },
                { account_id: 2, old_bal: 200, new_bal: 250 },
            ]);
    });

    it('a trigger body can use IF + RAISE to reject a row', () => {
        none(`create table t (id int, v int);
               create function guard() returns trigger as $$
                   begin if new.v < 0 then raise exception 'negative v: %', new.v; end if; return new; end;
               $$ language plpgsql;
               create trigger tg before insert on t for each row execute function guard();
               insert into t values (1, 5)`);
        expect(many(`select v from t`)[0].v).toEqual(5);
        expectQueryError(() => none(`insert into t values (2, -1)`), /negative v: -1/);
    });

    it('a statement-level trigger fires once per statement and can do DML', () => {
        none(`create table data (id int);
               create table log (msg text);
               create function audit_stmt() returns trigger as $$
                   begin insert into log(msg) values ('inserted'); return null; end;
               $$ language plpgsql;
               create trigger ds after insert on data for each statement
                   execute function audit_stmt();
               insert into data values (1), (2), (3);
               insert into data values (4)`);
        // two statements -> two log rows (regardless of row counts)
        expect(many(`select count(*)::int as c from log`)[0].c).toEqual(2);
    });

    it('exposes TG_OP, TG_TABLE_NAME, TG_ARGV and TG_NARGS', () => {
        none(`create table t (id int);
               create table log (tbl text, op text, a0 text, a1 text, nargs int);
               create function meta() returns trigger as $$
                   begin
                       insert into log(tbl, op, a0, a1, nargs)
                           values (tg_table_name, tg_op, tg_argv[0], tg_argv[1], tg_nargs);
                       return new;
                   end;
               $$ language plpgsql;
               create trigger tm after insert on t for each row execute function meta('x', 'y');
               insert into t values (1)`);
        expect(many(`select tbl, op, a0, a1, nargs from log`)).toEqual([
            { tbl: 't', op: 'INSERT', a0: 'x', a1: 'y', nargs: 2 },
        ]);
    });

    it('INSTEAD OF triggers make a view insertable/updatable/deletable', () => {
        none(`create table people (id int, first text, last text);
               create view full_names as select id, first || ' ' || last as name from people;
               create function v_ins() returns trigger as $$
                   begin insert into people(id, first, last)
                       values (new.id, split_part(new.name, ' ', 1), split_part(new.name, ' ', 2));
                       return new; end;
               $$ language plpgsql;
               create function v_upd() returns trigger as $$
                   begin update people set first = split_part(new.name, ' ', 1),
                       last = split_part(new.name, ' ', 2) where id = old.id; return new; end;
               $$ language plpgsql;
               create function v_del() returns trigger as $$
                   begin delete from people where id = old.id; return old; end;
               $$ language plpgsql;
               create trigger vi instead of insert on full_names for each row execute function v_ins();
               create trigger vu instead of update on full_names for each row execute function v_upd();
               create trigger vd instead of delete on full_names for each row execute function v_del();
               insert into full_names(id, name) values (1, 'Ada Lovelace'), (2, 'Alan Turing');
               update full_names set name = 'Ada King' where id = 1;
               delete from full_names where id = 2`);
        expect(many(`select id, first, last from people order by id`))
            .toEqual([{ id: 1, first: 'Ada', last: 'King' }]);
    });

    it('rejects a BEFORE trigger on a view and INSTEAD OF on a table', () => {
        none(`create table t (id int); create view v as select * from t;
               create function f() returns trigger as $$ begin return new; end; $$ language plpgsql`);
        expectQueryError(() => none(`create trigger x before insert on v for each row execute function f()`), /view|INSTEAD OF/i);
        expectQueryError(() => none(`create trigger y instead of insert on t for each row execute function f()`), /INSTEAD OF/i);
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
