-- @case: is distinct from with nulls
-- @expect: [{"a":false,"b":true,"c":false}]
select null is distinct from null as a, null is distinct from 1 as b, 1 is distinct from 1 as c;

-- @case: is not distinct from with nulls
-- @expect: [{"a":true,"b":false}]
select null is not distinct from null as a, null is not distinct from 1 as b;

-- @case: is distinct from with non-null operands
-- @expect: [{"a":true,"b":false}]
select 1 is distinct from 2 as a, 1 is distinct from 1 as b;

-- @case: filter with is distinct from over nullable columns
-- @expect: [{"c":2}]
create table t (a int, b int);
insert into t values (1, 1), (1, 2), (null, 1), (null, null);
select count(*)::int as c from t where a is distinct from b;
