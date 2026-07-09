-- @case: before update mutates NEW
-- @expect: [{"rev":2}]
create table t (id int, rev int);
insert into t values (1, 0);
create function bump() returns trigger as $$
begin
    new.rev = old.rev + 1;
    return new;
end;
$$ language plpgsql;
create trigger t_bump before update on t for each row execute function bump();
update t set id = id where id = 1;
update t set id = id where id = 1;
select rev from t;

-- @case: before insert mutates NEW with if/else
-- @expect: [{"id":2,"tag":"small"},{"id":10,"tag":"big"}]
create table t (id int, tag text);
create function tag_it() returns trigger as $$
begin
    if new.id > 5 then new.tag = 'big';
    else new.tag = 'small';
    end if;
    return new;
end;
$$ language plpgsql;
create trigger t_tag before insert on t for each row execute function tag_it();
insert into t(id) values (10), (2);
select id, tag from t order by id;

-- @case: before insert returning null skips the row
-- @expect: [{"id":1},{"id":3},{"id":5}]
create table t (id int);
create function no_evens() returns trigger as $$
begin
    if new.id % 2 = 0 then return null; end if;
    return new;
end;
$$ language plpgsql;
create trigger t_odd before insert on t for each row execute function no_evens();
insert into t values (1), (2), (3), (4), (5);
select id from t order by id;

-- @case: before delete returning null blocks the delete
-- @expect: [{"id":1}]
create table t (id int);
insert into t values (1), (2);
create function keep_one() returns trigger as $$
begin
    if old.id = 1 then return null; end if;
    return old;
end;
$$ language plpgsql;
create trigger t_keep before delete on t for each row execute function keep_one();
delete from t;
select id from t order by id;

-- @case: WHEN condition gates row-trigger firing
-- @expect: [{"id":1,"tag":null},{"id":2,"tag":"big"}]
create table t (id int, n int, tag text);
insert into t values (1, 5, null), (2, 15, null);
create function mark() returns trigger as $$
begin new.tag = 'big'; return new; end;
$$ language plpgsql;
create trigger t_when before update on t for each row
    when (new.n > 10) execute function mark();
update t set n = n;
select id, tag from t order by id;

-- @case: UPDATE OF fires only when a listed column changes
-- @expect: [{"hits":1}]
create table u (id int, a int, b int, hits int);
insert into u values (1, 1, 1, 0);
create function bump() returns trigger as $$
begin new.hits = old.hits + 1; return new; end;
$$ language plpgsql;
create trigger u_bump before update of a on u for each row execute function bump();
update u set b = 99 where id = 1;
update u set a = 42 where id = 1;
select hits from u;

-- @case: audit trigger inserts into a log table using NEW/OLD
-- @expect: [{"account_id":1,"old_bal":100,"new_bal":150}]
create table accounts (id int, balance int);
create table audit_log (account_id int, old_bal int, new_bal int);
create function log_change() returns trigger as $$
begin
    insert into audit_log(account_id, old_bal, new_bal)
        values (new.id, old.balance, new.balance);
    return new;
end; $$ language plpgsql;
create trigger acc_audit after update on accounts for each row execute function log_change();
insert into accounts values (1, 100);
update accounts set balance = 150 where id = 1;
select account_id, old_bal, new_bal from audit_log;

-- @case: statement-level trigger fires once per statement
-- @expect: [{"c":2}]
create table data (id int);
create table logt (msg text);
create function audit_stmt() returns trigger as $$
begin insert into logt(msg) values ('ins'); return null; end;
$$ language plpgsql;
create trigger ds after insert on data for each statement execute function audit_stmt();
insert into data values (1), (2), (3);
insert into data values (4);
select count(*)::int as c from logt;
