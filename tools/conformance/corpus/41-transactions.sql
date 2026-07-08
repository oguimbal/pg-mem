-- @case: savepoint rollback
-- @expect: [{"id":1}]
create table t (id int);
begin;
insert into t values (1);
savepoint sp1;
insert into t values (2);
rollback to savepoint sp1;
commit;
select * from t;

-- @case: deferrable constraint
create table a (id int primary key);
create table b (id int references a (id) deferrable initially deferred);
begin;
insert into b values (1);
insert into a values (1);
commit;
