-- @case: recursive counter
-- @expect: [{"n":1},{"n":2},{"n":3}]
with recursive c(n) as (
    select 1
    union all
    select n + 1 from c where n < 3
)
select n from c;

-- @case: recursive hierarchy
-- @expect: [{"id":1,"depth":0},{"id":2,"depth":1},{"id":3,"depth":2}]
create table org (id int, parent int);
insert into org values (1, null), (2, 1), (3, 2);
with recursive tree(id, parent, depth) as (
    select id, parent, 0 from org where parent is null
    union all
    select o.id, o.parent, t.depth + 1 from org o join tree t on o.parent = t.id
)
select id, depth from tree order by id;

-- @case: recursive cte without column list
-- @expect: [{"id":1,"depth":0},{"id":2,"depth":1}]
create table org (id int, parent int);
insert into org values (1, null), (2, 1);
with recursive tree as (
    select id, parent, 0 as depth from org where parent is null
    union all
    select o.id, o.parent, t.depth + 1 from org o join tree t on o.parent = t.id
)
select id, depth from tree order by id;
