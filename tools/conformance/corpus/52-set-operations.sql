-- @case: intersect distinct
-- @expect: [{"x":2},{"x":3}]
create table a (x int); create table b (x int);
insert into a values (1), (2), (2), (3);
insert into b values (2), (2), (3), (4);
select x from a intersect select x from b order by x;

-- @case: except distinct
-- @expect: [{"x":1}]
create table a (x int); create table b (x int);
insert into a values (1), (2), (2), (3);
insert into b values (2), (2), (3), (4);
select x from a except select x from b order by x;

-- @case: intersect all keeps min multiplicity
-- @expect: [{"x":2},{"x":2},{"x":3}]
create table a (x int); create table b (x int);
insert into a values (1), (2), (2), (3);
insert into b values (2), (2), (3), (4);
select x from a intersect all select x from b order by x;

-- @case: except all subtracts multiplicity
-- @expect: [{"x":1},{"x":2},{"x":2}]
create table a (x int); create table b (x int);
insert into a values (1), (2), (2), (2), (3);
insert into b values (2), (3), (4);
select x from a except all select x from b order by x;

-- @case: intersect treats nulls as equal
-- @expect: [{"x":null}]
create table c (x int); create table d (x int);
insert into c values (1), (null);
insert into d values (null);
select x from c intersect select x from d;

-- @case: chained set operations with parentheses
-- @expect: [{"x":2}]
create table a (x int); create table b (x int);
insert into a values (1), (2), (2), (3);
insert into b values (2), (2), (3), (4);
(select x from a intersect select x from b) except select 3 order by 1;
