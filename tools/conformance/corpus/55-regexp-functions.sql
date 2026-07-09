-- @case: regexp_matches capture groups
-- @expect: [{"m":["a","b"]}]
select regexp_matches('abc', '(a)(b)') as m;

-- @case: regexp_matches whole match when no groups
-- @expect: [{"m":["oo"]}]
select regexp_matches('foobar', 'o+') as m;

-- @case: regexp_matches global yields one row per match
-- @expect: [{"m":["a","1"]},{"m":["b","2"]},{"m":["c","3"]}]
select regexp_matches('a1b2c3', '([a-z])([0-9])', 'g') as m;

-- @case: regexp_split_to_array
-- @expect: [{"a":["a","b","c"]}]
select regexp_split_to_array('a1b2c', '[0-9]') as a;

-- @case: regexp_split_to_table
-- @expect: [{"p":"a"},{"p":"b"},{"p":"c"}]
select regexp_split_to_table('a,b,c', ',') as p;

-- @case: regexp_matches case-insensitive flag
-- @expect: [{"m":["A","B"]}]
select regexp_matches('ABC', '(a)(b)', 'i') as m;
