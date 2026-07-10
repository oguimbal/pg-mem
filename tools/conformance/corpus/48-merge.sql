-- @case: merge upsert (matched update, not-matched insert)
-- @expect: [{"id":1,"v":"A"},{"id":2,"v":"B"},{"id":3,"v":"c"},{"id":4,"v":"D"}]
create table target (id int primary key, v text);
create table source (id int, v text, del boolean);
insert into target values (1, 'a'), (2, 'b'), (3, 'c');
insert into source values (1, 'A', false), (2, 'B', true), (4, 'D', false);
merge into target t using source s on t.id = s.id
  when matched then update set v = s.v
  when not matched then insert (id, v) values (s.id, s.v);
select * from target order by id;

-- @case: merge conditional matched actions (delete then update)
-- @expect: [{"id":1,"v":"A"},{"id":3,"v":"c"},{"id":4,"v":"D"}]
create table target (id int primary key, v text);
create table source (id int, v text, del boolean);
insert into target values (1, 'a'), (2, 'b'), (3, 'c');
insert into source values (1, 'A', false), (2, 'B', true), (4, 'D', false);
merge into target t using source s on t.id = s.id
  when matched and s.del then delete
  when matched then update set v = s.v
  when not matched then insert (id, v) values (s.id, s.v);
select * from target order by id;

-- @case: merge do nothing on match leaves rows untouched
-- @expect: [{"id":1,"v":"a"},{"id":2,"v":"b"},{"id":3,"v":"c"}]
create table target (id int primary key, v text);
create table source (id int, v text);
insert into target values (1, 'a'), (2, 'b'), (3, 'c');
insert into source values (1, 'X'), (2, 'Y');
merge into target t using source s on t.id = s.id
  when matched then do nothing;
select * from target order by id;

-- @case: merge from a subquery source
-- @expect: [{"id":1,"v":"A"},{"id":2,"v":"b"},{"id":4,"v":"D"}]
create table target (id int primary key, v text);
create table source (id int, v text, del boolean);
insert into target values (1, 'a'), (2, 'b');
insert into source values (1, 'A', false), (2, 'B', true), (4, 'D', false);
merge into target t using (select id, v from source where not del) s on t.id = s.id
  when matched then update set v = s.v
  when not matched then insert (id, v) values (s.id, s.v);
select * from target order by id;

-- @case: merge insert with default column value
-- @expect: [{"id":1,"v":"a"},{"id":2,"v":"def"}]
create table target (id int primary key, v text default 'def');
create table source (id int);
insert into target values (1, 'a');
insert into source values (1), (2);
merge into target t using source s on t.id = s.id
  when not matched then insert (id) values (s.id);
select * from target order by id;
