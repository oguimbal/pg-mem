-- @case: div / gcd / lcm / factorial
-- @expect: [{"a":3,"b":4,"c":12,"d":120}]
select div(7, 2) as a, gcd(12, 8) as b, lcm(4, 6) as c, factorial(5) as d;

-- @case: width_bucket and bit_length
-- @expect: [{"a":2,"b":24}]
select width_bucket(5, 1, 10, 3) as a, bit_length('abc') as b;

-- @case: bitwise operators
-- @expect: [{"a":1,"b":7,"c":4,"d":20,"e":5}]
select 5 & 3 as a, 5 | 2 as b, 5 # 1 as c, 5 << 2 as d, 20 >> 2 as e;

-- @case: exponentiation operator
-- @expect: [{"v":1024}]
select (2 ^ 10)::int as v;
