-- @case: count and sum with FILTER
-- @expect: [{"c":3,"s":7}]
create table t (x int, g text);
insert into t values (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b');
select count(*) filter (where x > 1) as c, sum(x) filter (where x > 2) as s from t;

-- @case: filtered and unfiltered aggregates in one query
-- @expect: [{"c":4,"f":2}]
create table t (x int);
insert into t values (1), (2), (3), (4);
select count(*) as c, count(*) filter (where x > 2) as f from t;

-- @case: FILTER with GROUP BY
-- @expect: [{"g":"a","c":1},{"g":"b","c":2}]
create table t (x int, g text);
insert into t values (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b');
select g, count(*) filter (where x > 1) as c from t group by g order by g;

-- @case: FILTER matching nothing yields 0
-- @expect: [{"c":0}]
create table t (x int);
insert into t values (1), (2);
select count(*) filter (where x > 10) as c from t;

-- @case: FILTER combined with DISTINCT
-- @expect: [{"c":3}]
create table t (x int);
insert into t values (1), (2), (2), (3), (4);
select count(distinct x) filter (where x > 1) as c from t;
