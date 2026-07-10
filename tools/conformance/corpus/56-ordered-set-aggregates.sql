-- @case: percentile_cont interpolates the median
-- @expect: [{"p":2.5}]
select percentile_cont(0.5) within group (order by x) as p from (values (1), (2), (3), (4)) v(x);

-- @case: percentile_cont at a quartile
-- @expect: [{"p":2}]
select percentile_cont(0.25) within group (order by x) as p from (values (1), (2), (3), (4), (5)) v(x);

-- @case: percentile_disc picks an existing value
-- @expect: [{"p":2}]
select percentile_disc(0.5) within group (order by x) as p from (values (1), (2), (3), (4)) v(x);

-- @case: mode returns the most frequent value
-- @expect: [{"m":1}]
select mode() within group (order by x) as m from (values (1), (1), (2), (3)) v(x);

-- @case: ordered-set aggregate with GROUP BY
-- @expect: [{"g":"a","p":2.5},{"g":"b","p":10}]
create table t (x int, g text);
insert into t values (1,'a'),(2,'a'),(3,'a'),(4,'a'),(10,'b'),(10,'b'),(20,'b');
select g, percentile_cont(0.5) within group (order by x) as p from t group by g order by g;
