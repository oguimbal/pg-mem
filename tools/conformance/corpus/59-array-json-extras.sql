-- @case: array_remove
-- @expect: [{"r":[2,3]}]
select array_remove(array[1, 2, 1, 3], 1) as r;

-- @case: array_remove with null
-- @expect: [{"r":[1,2]}]
select array_remove(array[1, null, 2, null]::int[], null) as r;

-- @case: array_replace
-- @expect: [{"r":[9,2,9]}]
select array_replace(array[1, 2, 1], 1, 9) as r;

-- @case: row_to_json
-- @expect: [{"j":{"a":1,"b":"x"}}]
select row_to_json(t) as j from (select 1 a, 'x' b) t;

-- @case: array_to_json
-- @expect: [{"j":[1,2,3]}]
select array_to_json(array[1, 2, 3]) as j;
