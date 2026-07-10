-- @case: week interval added to a date
-- @expect: [{"d":"2020-01-15"}]
select to_char(date '2020-01-01' + interval '2 weeks', 'YYYY-MM-DD') as d;

-- @case: decade interval added to a date
-- @expect: [{"d":"2030-01-01"}]
select to_char(date '2020-01-01' + interval '1 decade', 'YYYY-MM-DD') as d;

-- @case: extract days from a mixed week/day interval
-- @expect: [{"d":17}]
select extract(day from interval '2 weeks 3 days')::int as d;

-- @case: nth_value over the default frame
-- @expect: [{"x":10,"v":null},{"x":20,"v":20},{"x":30,"v":20}]
select x, nth_value(x, 2) over (order by x) as v from (values (10), (20), (30)) t(x) order by x;

-- @case: cume_dist
-- @expect: [{"x":1,"c":0.25},{"x":2,"c":0.75},{"x":2,"c":0.75},{"x":3,"c":1}]
select x, cume_dist() over (order by x) as c from (values (1), (2), (2), (3)) t(x) order by x;

-- @case: percent_rank
-- @expect: [{"x":1,"p":0},{"x":3,"p":0.5},{"x":3,"p":0.5}]
select x, percent_rank() over (order by x) as p from (values (1), (3), (3)) t(x) order by x;
