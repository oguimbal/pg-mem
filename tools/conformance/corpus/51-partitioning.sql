-- @case: range partition parent union
-- @expect: [{"id":1},{"id":2},{"id":3}]
create table events (id int, ts date) partition by range (ts);
create table events_2020 partition of events for values from ('2020-01-01') to ('2021-01-01');
create table events_2021 partition of events for values from ('2021-01-01') to ('2022-01-01');
insert into events values (1, '2020-06-01'), (2, '2021-03-01'), (3, '2020-12-31');
select id from events order by id;

-- @case: range partition child slice
-- @expect: [{"id":1},{"id":3}]
create table events (id int, ts date) partition by range (ts);
create table events_2020 partition of events for values from ('2020-01-01') to ('2021-01-01');
create table events_2021 partition of events for values from ('2021-01-01') to ('2022-01-01');
insert into events values (1, '2020-06-01'), (2, '2021-03-01'), (3, '2020-12-31');
select id from events_2020 order by id;

-- @case: parent insert with no matching partition errors
-- @error: no partition of relation "events" found for row
create table events (id int, ts date) partition by range (ts);
create table events_2020 partition of events for values from ('2020-01-01') to ('2021-01-01');
insert into events values (9, '2019-01-01');

-- @case: list partition routing (child)
-- @expect: [{"id":1},{"id":3}]
create table items (id int, cat text) partition by list (cat);
create table items_ab partition of items for values in ('a', 'b');
create table items_c partition of items for values in ('c');
insert into items values (1, 'a'), (2, 'c'), (3, 'b');
select id from items_ab order by id;

-- @case: list partition parent union
-- @expect: [{"id":1},{"id":2},{"id":3}]
create table items (id int, cat text) partition by list (cat);
create table items_ab partition of items for values in ('a', 'b');
create table items_c partition of items for values in ('c');
insert into items values (1, 'a'), (2, 'c'), (3, 'b');
select id from items order by id;

-- @case: default partition catches unmatched rows
-- @expect: [{"id":2},{"id":4}]
create table t (id int, cat text) partition by list (cat);
create table t_ab partition of t for values in ('a', 'b');
create table t_def partition of t default;
insert into t values (1, 'a'), (2, 'z'), (3, 'b'), (4, 'q');
select id from t_def order by id;

-- @case: direct child insert violating its bound errors
-- @error: partition
create table items (id int, cat text) partition by list (cat);
create table items_c partition of items for values in ('c');
insert into items_c values (5, 'a');
