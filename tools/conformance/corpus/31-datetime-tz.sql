-- @case: set timezone then read
-- @expect: [{"TimeZone":"UTC"}]
set timezone = 'UTC';
show timezone;

-- @case: at time zone
-- @expect: [{"r":"2020-01-01T12:00:00.000Z"}]
select timestamp '2020-01-01 12:00:00' at time zone 'UTC' as r;

-- @case: timestamptz cast conversion
-- @expect: [{"r":"2020-01-01 07:00:00+00"}]
set timezone = 'UTC';
select ('2020-01-01 12:00:00+05'::timestamptz)::text as r;
