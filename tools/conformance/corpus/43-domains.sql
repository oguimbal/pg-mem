-- @case: domain check accepts a valid value
-- @expect: [{"qty":5}]
create domain posint as int check (value > 0);
create table t (id int, qty posint);
insert into t values (1, 5);
select qty from t;

-- @case: domain check rejects an invalid value
-- @error: violates check constraint
create domain posint as int check (value > 0);
create table t (id int, qty posint);
insert into t values (1, -3);

-- @case: domain value behaves as its base type
-- @expect: [{"r":15}]
create domain posint as int check (value > 0);
create table t (id int, qty posint);
insert into t values (1, 5);
select qty + 10 as r from t where id = 1;

-- @case: domain not null is enforced
-- @error: null
create domain nzt as text not null;
create table t (v nzt);
insert into t values (null);

-- @case: domain check enforced on explicit cast
-- @error: violates check constraint
create domain posint as int check (value > 0);
select (-1)::posint;
