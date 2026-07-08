-- @case: abs
-- @expect: [{"r":17.4}]
select abs(-17.4) as r;

-- @case: ceil floor
-- @expect: [{"a":-42,"b":-43}]
select ceil(-42.8) as a, floor(-42.8) as b;

-- @case: round
-- @expect: [{"r":42}]
select round(42.4) as r;

-- @case: round with scale
-- @expect: [{"r":42.44}]
select round(42.4382, 2) as r;

-- @case: power sqrt
-- @expect: [{"a":1024,"b":1.4142135623730951}]
select power(2, 10) as a, sqrt(2) as b;

-- @case: mod
-- @expect: [{"r":1}]
select mod(9, 4) as r;

-- @case: sign
-- @expect: [{"r":-1}]
select sign(-8.4) as r;

-- @case: trunc
-- @expect: [{"r":42}]
select trunc(42.8) as r;

-- @case: exp ln
-- pg parses the ln() literal as numeric, so ln() is computed in arbitrary precision
-- @expect: [{"a":2.718281828459045,"b":"0.9999999999999999"}]
select exp(1) as a, ln(2.718281828459045) as b;

-- @case: pi
-- @expect: [{"r":3.141592653589793}]
select pi() as r;

-- @case: greatest least
-- @expect: [{"a":3,"b":1}]
select greatest(1, 2, 3) as a, least(3, 2, 1) as b;
