-- @case: ROLLUP subtotals and grand total
-- @expect: [{"g":"a","sub":"x","s":1},{"g":"a","sub":"y","s":2},{"g":"a","sub":null,"s":3},{"g":"b","sub":"x","s":3},{"g":"b","sub":null,"s":3},{"g":null,"sub":null,"s":6}]
create table s (g text, sub text, amt int);
insert into s values ('a','x',1), ('a','y',2), ('b','x',3);
select g, sub, sum(amt)::int as s from s
  group by rollup(g, sub)
  order by g nulls last, sub nulls last;

-- @case: CUBE all combinations
-- @expect: [{"g":"a","sub":"x","s":1},{"g":"a","sub":"y","s":2},{"g":"a","sub":null,"s":3},{"g":"b","sub":"x","s":3},{"g":"b","sub":null,"s":3},{"g":null,"sub":"x","s":4},{"g":null,"sub":"y","s":2},{"g":null,"sub":null,"s":6}]
create table s (g text, sub text, amt int);
insert into s values ('a','x',1), ('a','y',2), ('b','x',3);
select g, sub, sum(amt)::int as s from s
  group by cube(g, sub)
  order by g nulls last, sub nulls last;

-- @case: plain column mixed with ROLLUP
-- @expect: [{"g":"a","sub":"x","s":1},{"g":"a","sub":"y","s":2},{"g":"a","sub":null,"s":3},{"g":"b","sub":"x","s":3},{"g":"b","sub":null,"s":3}]
create table s (g text, sub text, amt int);
insert into s values ('a','x',1), ('a','y',2), ('b','x',3);
select g, sub, sum(amt)::int as s from s
  group by g, rollup(sub)
  order by g, sub nulls last;
