-- @case: prepared select with typed parameter
-- @expect: [{"r":42}]
prepare p(int) as select $1 + 1 as r;
execute p(41);

-- @case: prepared statement with an expression argument
-- @expect: [{"r":10}]
prepare dbl(int) as select $1 * 2 as r;
execute dbl(2 + 3);

-- @case: prepared insert then read back
-- @expect: [{"id":1,"name":"alice"},{"id":2,"name":"bob"}]
create table users (id int primary key, name text);
prepare addu(int, text) as insert into users values ($1, $2);
execute addu(1, 'alice');
execute addu(2, 'bob');
select id, name from users order by id;

-- @case: parameter compared to an indexed column
-- @expect: [{"name":"bob"}]
create table u2 (id int primary key, name text);
insert into u2 values (1, 'alice'), (2, 'bob');
prepare getu(int) as select name from u2 where id = $1;
execute getu(2);

-- @case: deallocate removes a prepared statement
-- @error: does not exist
prepare gone as select 1;
deallocate gone;
execute gone;
