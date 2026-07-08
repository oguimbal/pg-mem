-- @case: generate_series
-- @expect: [{"generate_series":1},{"generate_series":2},{"generate_series":3}]
select * from generate_series(1, 3);

-- @case: unnest
-- @expect: [{"unnest":1},{"unnest":2}]
select unnest(array[1, 2]);

-- @case: array_length
-- @expect: [{"r":3}]
select array_length(array[1, 2, 3], 1) as r;

-- @case: cardinality
-- @expect: [{"r":3}]
select cardinality(array[1, 2, 3]) as r;

-- @case: array_append array_cat
-- @expect: [{"a":[1,2,3],"b":[1,2,3,4]}]
select array_append(array[1, 2], 3) as a, array_cat(array[1, 2], array[3, 4]) as b;

-- @case: string_to_array
-- @expect: [{"r":["a","b","c"]}]
select string_to_array('a,b,c', ',') as r;

-- @case: array_to_string
-- @expect: [{"r":"1,2,3"}]
select array_to_string(array[1, 2, 3], ',') as r;

-- @case: array_position
-- @expect: [{"r":2}]
select array_position(array['a', 'b', 'c'], 'b') as r;

-- @case: any of array
-- @expect: [{"r":true}]
select 2 = any(array[1, 2, 3]) as r;
