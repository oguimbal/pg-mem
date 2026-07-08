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
