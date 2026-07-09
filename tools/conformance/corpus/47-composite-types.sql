-- @case: composite field access
-- @expect: [{"cx":3,"cy":4}]
create type pt as (x int, y int);
create table shapes (id int, center pt);
insert into shapes values (1, row(3, 4)::pt);
select (center).x as cx, (center).y as cy from shapes where id = 1;

-- @case: filter on composite field
-- @expect: [{"id":2}]
create type pt as (x int, y int);
create table shapes (id int, center pt);
insert into shapes values (1, row(3, 4)::pt), (2, row(10, 20)::pt);
select id from shapes where (center).x > 5 order by id;

-- @case: composite text field
-- @expect: [{"c":"Springfield"}]
create type addr as (street text, city text);
create table people (id int, home addr);
insert into people values (1, row('1 Main', 'Springfield')::addr);
select (home).city as c from people;

-- @case: arithmetic on composite fields
-- @expect: [{"s":37}]
create type pt as (x int, y int);
create table shapes (id int, center pt);
insert into shapes values (1, row(3, 4)::pt), (2, row(10, 20)::pt);
select sum((center).x + (center).y) as s from shapes;

-- @case: duplicate composite type name rejected
-- @error: already exists
create type pt as (x int, y int);
create type pt as (a int);
