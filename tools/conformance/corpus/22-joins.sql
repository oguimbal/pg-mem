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
