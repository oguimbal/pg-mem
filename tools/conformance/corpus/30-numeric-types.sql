-- @case: bigint 64-bit precision
-- @expect: [{"r":"9007199254740993"}]
select (9007199254740993::bigint)::text as r;

-- @case: numeric scale rounding
-- @expect: [{"r":"1.01"}]
select (1.005::numeric(10,2))::text as r;

-- @case: numeric round half away from zero
-- @expect: [{"r":-3}]
select round(-2.5::numeric) as r;

-- @case: integer division truncates
-- @expect: [{"r":3}]
select 7 / 2 as r;

-- @case: integer overflow errors
-- @error: integer out of range
select 2147483647::int + 1;

-- @case: numeric division keeps precision
-- @expect: [{"r":"0.33333333333333333333"}]
select (1::numeric / 3::numeric)::text as r;
