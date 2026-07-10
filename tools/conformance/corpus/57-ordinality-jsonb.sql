-- @case: unnest with ordinality
-- @expect: [{"unnest":"a","ordinality":"1"},{"unnest":"b","ordinality":"2"},{"unnest":"c","ordinality":"3"}]
select * from unnest(array['a', 'b', 'c']) with ordinality;

-- @case: unnest with ordinality and column aliases
-- @expect: [{"idx":"2","val":"y"},{"idx":"1","val":"x"}]
select idx, val from unnest(array['x', 'y']) with ordinality as u(val, idx) order by idx desc;

-- @case: jsonb #- removes an object key
-- @expect: [{"r":{"b":2}}]
select '{"a":1,"b":2}'::jsonb #- '{a}' as r;

-- @case: jsonb #- removes an array element
-- @expect: [{"r":[1,3]}]
select '[1,2,3]'::jsonb #- '{1}' as r;

-- @case: jsonb #- removes a nested path
-- @expect: [{"r":{"a":{"c":3}}}]
select '{"a":{"b":2,"c":3}}'::jsonb #- '{a,b}' as r;

-- @case: jsonb_pretty formats with indentation
-- @expect: [{"r":"{\n    \"a\": 1\n}"}]
select jsonb_pretty('{"a":1}'::jsonb) as r;
