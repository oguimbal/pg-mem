-- @case: date_trunc on timestamptz
-- @expect: [{"d":"2020-06-01"}]
select to_char(date_trunc('month', timestamptz '2020-06-15 10:30:00+00'), 'YYYY-MM-DD') as d;

-- @case: date_part on timestamptz
-- @expect: [{"y":2020}]
select date_part('year', timestamptz '2020-06-15 00:00:00+00') as y;

-- @case: to_timestamp from epoch seconds
-- @expect: [{"d":"2020-01-01"}]
select to_char(to_timestamp(1577836800), 'YYYY-MM-DD') as d;

-- @case: to_char on a date value
-- @expect: [{"d":"2020-03-15"}]
select to_char(date '2020-03-15', 'YYYY-MM-DD') as d;

-- @case: date_trunc to the day on timestamptz
-- @expect: [{"d":"2020-06-15"}]
select to_char(date_trunc('day', timestamptz '2020-06-15 23:59:00+00'), 'YYYY-MM-DD') as d;
