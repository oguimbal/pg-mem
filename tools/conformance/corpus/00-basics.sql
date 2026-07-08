-- Sanity cases: these should all pass. If one fails, the harness or a regression is at fault.

-- @case: create insert select
-- @expect: [{"id":1,"name":"a"}]
create table t (id int primary key, name text);
insert into t values (1, 'a');
select * from t;

-- @case: group by with count
-- @expect: [{"k":"a","count":2},{"k":"b","count":1}]
create table t (k text);
insert into t values ('a'), ('a'), ('b');
select k, count(*) as count from t group by k order by k;

-- @case: on conflict do update
-- @expect: [{"id":1,"v":"new"}]
create table t (id int primary key, v text);
insert into t values (1, 'old');
insert into t values (1, 'new') on conflict (id) do update set v = excluded.v;
select * from t;

-- @case: returning clause
-- @expect: [{"id":1}]
create table t (id int primary key);
insert into t values (1) returning id;

-- @case: simple cte
-- @expect: [{"x":42}]
with c as (select 42 as x) select * from c;

-- @case: union all
-- @expect: [{"x":1},{"x":2}]
select 1 as x union all select 2 as x;

-- @case: left join
-- @expect: [{"id":1,"v":"x"},{"id":2,"v":null}]
create table a (id int);
create table b (id int, v text);
insert into a values (1), (2);
insert into b values (1, 'x');
select a.id, b.v from a left join b on a.id = b.id order by a.id;

-- @case: case expression
-- @expect: [{"r":"small"}]
select case when 1 < 10 then 'small' else 'big' end as r;

-- @case: coalesce and nullif
-- @expect: [{"a":1,"b":null}]
select coalesce(null, 1) as a, nullif(5, 5) as b;

-- @case: limit offset
-- @expect: [{"x":2}]
select x from (values (1), (2), (3)) v(x) order by x limit 1 offset 1;
