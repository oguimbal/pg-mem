-- @case: length
-- @expect: [{"r":5}]
select length('hello') as r;

-- @case: char_length
-- @expect: [{"r":4}]
select char_length('jose') as r;

-- @case: substr
-- @expect: [{"r":"ph"}]
select substr('alphabet', 3, 2) as r;

-- @case: replace
-- @expect: [{"r":"abXXefabXXef"}]
select replace('abcdefabcdef', 'cd', 'XX') as r;

-- @case: trim
-- @expect: [{"r":"hi"}]
select trim('  hi  ') as r;

-- @case: ltrim rtrim
-- @expect: [{"a":"test","b":"test"}]
select ltrim('zzzytest', 'xyz') as a, rtrim('testxxzx', 'xyz') as b;

-- @case: btrim
-- @expect: [{"r":"trim"}]
select btrim('xyxtrimyyx', 'xyz') as r;

-- @case: lpad rpad
-- @expect: [{"a":"xyxhi","b":"hixyx"}]
select lpad('hi', 5, 'xy') as a, rpad('hi', 5, 'xy') as b;

-- @case: split_part
-- @expect: [{"r":"def"}]
select split_part('abc~@~def~@~ghi', '~@~', 2) as r;

-- @case: strpos
-- @expect: [{"r":3}]
select strpos('Thomas', 'om') as r;

-- @case: position in
-- @expect: [{"r":3}]
select position('om' in 'Thomas') as r;

-- @case: initcap
-- @expect: [{"r":"Hi Thomas"}]
select initcap('hi THOMAS') as r;

-- @case: reverse
-- @expect: [{"r":"edcba"}]
select reverse('abcde') as r;

-- @case: left right
-- @expect: [{"a":"ab","b":"de"}]
select left('abcde', 2) as a, right('abcde', 2) as b;

-- @case: repeat
-- @expect: [{"r":"PgPgPg"}]
select repeat('Pg', 3) as r;

-- @case: md5
-- @expect: [{"r":"900150983cd24fb0d6963f7d28e17f72"}]
select md5('abc') as r;

-- @case: ascii chr
-- @expect: [{"a":120,"b":"A"}]
select ascii('x') as a, chr(65) as b;

-- @case: translate
-- @expect: [{"r":"a2x5"}]
select translate('12345', '143', 'ax') as r;

-- @case: starts_with
-- @expect: [{"r":true}]
select starts_with('alphabet', 'alph') as r;

-- @case: regexp_replace
-- @expect: [{"r":"ThM"}]
select regexp_replace('Thomas', '.[mN]a.', 'M') as r;

-- @case: to_char number
-- @expect: [{"r":" 125"}]
select to_char(125, '999') as r;

-- @case: format
-- @expect: [{"r":"Hello, world"}]
select format('Hello, %s', 'world') as r;
