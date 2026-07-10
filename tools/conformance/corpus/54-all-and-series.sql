-- @case: <> ALL and = ALL
-- @expect: [{"a":true,"b":true,"c":false}]
select 1 <> all(array[2, 3]) as a, 1 = all(array[1, 1]) as b, 1 = all(array[1, 2]) as c;

-- @case: comparison quantifier > ALL
-- @expect: [{"a":true,"b":false}]
select 5 > all(array[1, 2, 3]) as a, 5 > all(array[1, 9]) as b;

-- @case: filter a table with > ALL
-- @expect: [{"x":5},{"x":10}]
create table t (x int);
insert into t values (1), (5), (10);
select x from t where x > all(array[2, 3]) order by x;

-- @case: generate_series over timestamps by day
-- @expect: [{"d":"2020-01-01"},{"d":"2020-01-02"},{"d":"2020-01-03"}]
select to_char(g, 'YYYY-MM-DD') as d
from generate_series(timestamp '2020-01-01', timestamp '2020-01-03', interval '1 day') g;

-- @case: generate_series over timestamps by month
-- @expect: [{"d":"2020-01-01"},{"d":"2020-02-01"},{"d":"2020-03-01"}]
select to_char(g, 'YYYY-MM-DD') as d
from generate_series(timestamp '2020-01-01', timestamp '2020-03-01', interval '1 month') g;

-- @case: generate_series descending
-- @expect: [{"d":"2020-01-03"},{"d":"2020-01-02"},{"d":"2020-01-01"}]
select to_char(g, 'YYYY-MM-DD') as d
from generate_series(timestamp '2020-01-03', timestamp '2020-01-01', interval '-1 day') g;
