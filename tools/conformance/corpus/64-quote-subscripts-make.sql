-- @case: quote_ident
-- @expect: [{"a":"\"foo bar\"","b":"foo"}]
select quote_ident('foo bar') as a, quote_ident('foo') as b;

-- @case: quote_literal and quote_nullable
-- @expect: [{"a":"'a''b'","b":"NULL"}]
select quote_literal('a''b') as a, quote_nullable(null) as b;

-- @case: generate_subscripts
-- @expect: [{"v":1},{"v":2},{"v":3}]
select generate_subscripts(array[10, 20, 30], 1) as v;

-- @case: make_timestamp
-- @expect: [{"v":"2020-06-15 10:30"}]
select to_char(make_timestamp(2020, 6, 15, 10, 30, 0), 'YYYY-MM-DD HH24:MI') as v;

-- @case: make_time
-- @expect: [{"v":"10:30:00"}]
select make_time(10, 30, 0) as v;
