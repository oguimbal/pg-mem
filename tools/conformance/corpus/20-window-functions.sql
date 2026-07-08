-- @case: row_number over
-- @expect: [{"x":10,"n":1},{"x":20,"n":2}]
select x, row_number() over (order by x) as n from (values (20), (10)) v(x) order by x;

-- @case: rank with partition
create table t (grp text, v int);
insert into t values ('a', 1), ('a', 2), ('b', 3);
select grp, v, rank() over (partition by grp order by v) as r from t;

-- @case: lag lead
select x, lag(x) over (order by x) as prev, lead(x) over (order by x) as next
from (values (1), (2), (3)) v(x);

-- @case: sum over partition
select x, sum(x) over () as total from (values (1), (2), (3)) v(x);

-- @case: ntile
select x, ntile(2) over (order by x) as bucket from (values (1), (2), (3), (4)) v(x);

-- @case: window frame rows between
select x, sum(x) over (order by x rows between 1 preceding and current row) as s
from (values (1), (2), (3)) v(x);
