-- @case: int4range canonical rendering
-- @expect: [{"r":"[1,10)"}]
select int4range(1, 10) as r;

-- @case: int4range inclusive bounds canonicalize to [)
-- @expect: [{"r":"[1,11)"}]
select int4range(1, 10, '[]') as r;

-- @case: int4range literal with exclusive lower canonicalizes
-- @expect: [{"r":"[2,11)"}]
select '(1,10]'::int4range as r;

-- @case: empty range
-- @expect: [{"e":true}]
select isempty(int4range(5, 5)) as e;

-- @case: range contains element
-- @expect: [{"a":true,"b":false}]
select int4range(1, 10) @> 5 as a, int4range(1, 10) @> 10 as b;

-- @case: element contained by range
-- @expect: [{"c":true}]
select 5 <@ int4range(1, 10) as c;

-- @case: range contains range
-- @expect: [{"c":true}]
select int4range(1, 10) @> int4range(2, 5) as c;

-- @case: range overlap
-- @expect: [{"a":true,"b":false}]
select int4range(1, 10) && int4range(5, 20) as a, int4range(1, 5) && int4range(10, 20) as b;

-- @case: lower and upper accessors
-- @expect: [{"l":3,"u":10}]
select lower(int4range(3, 10)) as l, upper(int4range(3, 10)) as u;

-- @case: lower_inc / upper_inc
-- @expect: [{"li":true,"ui":false}]
select lower_inc(int4range(3, 10)) as li, upper_inc(int4range(3, 10)) as ui;

-- @case: numeric range containment
-- @expect: [{"c":true}]
select numrange(1.5, 3.0) @> 2.0 as c;

-- @case: date range containment
-- @expect: [{"a":true,"b":false}]
select '[2020-01-01,2020-02-01)'::daterange @> '2020-01-15'::date as a,
       '[2020-01-01,2020-02-01)'::daterange @> '2020-03-01'::date as b;

-- @case: range column filtered with @>
-- @expect: [{"id":1},{"id":2}]
create table t (id int, r int4range);
insert into t values (1, int4range(1, 10)), (2, '[5,20)'::int4range);
select id from t where r @> 7 order by id;
