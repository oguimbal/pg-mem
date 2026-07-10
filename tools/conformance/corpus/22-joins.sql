-- @case: full outer join
-- @expect: [{"a":1,"b":null},{"a":2,"b":2},{"a":null,"b":3}]
create table ta (a int);
create table tb (b int);
insert into ta values (1), (2);
insert into tb values (2), (3);
select a, b from ta full outer join tb on a = b order by coalesce(a, b);

-- @case: lateral join
create table t (id int, arr int[]);
insert into t values (1, array[10, 20]);
select id, x from t, lateral unnest(arr) as x;

-- @case: cross join
-- @expect: [{"a":1,"b":3},{"a":1,"b":4},{"a":2,"b":3},{"a":2,"b":4}]
select a, b from (values (1), (2)) va(a) cross join (values (3), (4)) vb(b) order by a, b;

-- @case: using clause
-- @expect: [{"id":1,"x":"a","y":"b"}]
create table ta (id int, x text);
create table tb (id int, y text);
insert into ta values (1, 'a');
insert into tb values (1, 'b');
select id, x, y from ta join tb using (id);

-- @case: correlated EXISTS subquery
-- @expect: [{"id":1},{"id":2}]
create table co_a (id int, pid int);
create table co_b (id int);
insert into co_a values (1,10),(2,20),(3,99);
insert into co_b values (10),(20);
select id from co_a a where exists (select 1 from co_b b where b.id = a.pid) order by id;

-- @case: correlated scalar subquery in select list
-- @expect: [{"id":1,"n":1},{"id":2,"n":1},{"id":3,"n":0}]
select id, (select count(*)::int from co_b b where b.id = a.pid) as n from co_a a order by id;

-- @case: scalar subquery (single value)
-- @expect: [{"n":2}]
select (select count(*)::int from co_b) as n;

-- @case: IN with a subquery
-- @expect: [{"id":1},{"id":2}]
select id from co_a where pid in (select id from co_b) order by id;
