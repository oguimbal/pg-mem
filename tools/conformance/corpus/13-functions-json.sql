-- @case: jsonb arrow operators
-- @expect: [{"a":{"b":1},"b":"1"}]
select '{"a":{"b":1}}'::jsonb -> 'a' as a, '{"a":{"b":1}}'::jsonb #>> '{a,b}' as b;

-- @case: json_build_object
-- @expect: [{"r":{"a":1,"b":"x"}}]
select json_build_object('a', 1, 'b', 'x') as r;

-- @case: jsonb_array_length
-- @expect: [{"r":3}]
select jsonb_array_length('[1,2,3]'::jsonb) as r;

-- @case: jsonb_set
-- @expect: [{"r":{"a":2}}]
select jsonb_set('{"a":1}'::jsonb, '{a}', '2'::jsonb) as r;

-- @case: to_jsonb
-- @expect: [{"r":"txt"}]
select to_jsonb('txt'::text) as r;

-- @case: jsonb_object_keys
-- @expect: [{"k":"a"},{"k":"b"}]
select jsonb_object_keys('{"a":1,"b":2}'::jsonb) as k;

-- @case: jsonb_each rows
select * from jsonb_each('{"a":1}'::jsonb);

-- @case: json_agg
-- @expect: [{"r":[1,2]}]
select json_agg(x) as r from (values (1), (2)) v(x);
