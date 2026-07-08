-- @case: set timezone then read
-- @expect: [{"TimeZone":"UTC"}]
set timezone = 'UTC';
show timezone;

-- @case: at time zone
select timestamp '2020-01-01 12:00:00' at time zone 'UTC' as r;

-- @case: timestamptz cast conversion
set timezone = 'UTC';
select ('2020-01-01 12:00:00+05'::timestamptz)::text as r;
