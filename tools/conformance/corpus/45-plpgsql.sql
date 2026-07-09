-- @case: plpgsql scalar function with IF/ELSIF/ELSE
-- @expect: [{"a":"pos","b":"neg","c":"zero"}]
create function classify(n int) returns text as $$
begin
    if n > 0 then return 'pos';
    elsif n < 0 then return 'neg';
    else return 'zero';
    end if;
end; $$ language plpgsql;
select classify(5) as a, classify(-2) as b, classify(0) as c;

-- @case: plpgsql FOR loop accumulation
-- @expect: [{"r":5050}]
create function sumto(n int) returns int as $$
declare s int := 0; i int;
begin for i in 1..n loop s := s + i; end loop; return s; end;
$$ language plpgsql;
select sumto(100) as r;

-- @case: plpgsql recursion
-- @expect: [{"r":120}]
create function fact(n int) returns int as $$
begin if n <= 1 then return 1; end if; return n * fact(n - 1); end;
$$ language plpgsql;
select fact(5) as r;

-- @case: plpgsql WHILE with EXIT and CONTINUE WHEN
-- @expect: [{"r":5}]
create function countodd(n int) returns int as $$
declare c int := 0; i int;
begin
    for i in 1..n loop
        continue when i % 2 = 0;
        c := c + 1;
    end loop;
    return c;
end; $$ language plpgsql;
select countodd(10) as r;

-- @case: plpgsql SELECT INTO with a parameter
-- @expect: [{"r":2}]
create table nums (v int);
insert into nums values (10), (20), (30);
create function cntgt(threshold int) returns int as $$
declare c int;
begin select count(*)::int into c from nums where v > threshold; return c; end;
$$ language plpgsql;
select cntgt(15) as r;

-- @case: plpgsql embedded INSERT (void function)
-- @expect: [{"c":4}]
create table items (id int, v int);
insert into items(id, v) values (1,10),(2,20),(3,30);
create function addrow(pid int, pv int) returns void as $$
begin insert into items(id, v) values (pid, pv); end;
$$ language plpgsql;
select addrow(4, 40);
select count(*)::int as c from items;

-- @case: plpgsql FOUND after SELECT INTO
-- @expect: [{"a":true,"b":false}]
create table people (id int);
insert into people values (1), (2);
create function has_id(pid int) returns boolean as $$
declare x int;
begin select id into x from people where id = pid; return found; end;
$$ language plpgsql;
select has_id(2) as a, has_id(99) as b;

-- @case: plpgsql RAISE EXCEPTION
-- @error: negative
create function checkpos(n int) returns int as $$
begin if n < 0 then raise exception 'negative: %', n; end if; return n; end;
$$ language plpgsql;
select checkpos(-3);

-- @case: plpgsql EXCEPTION WHEN with rollback
-- @expect: [{"present":0}]
create table uu (id int primary key);
insert into uu values (1);
create function safeadd(pid int) returns text as $$
begin
    insert into uu(id) values (pid);
    insert into uu(id) values (pid);
    return 'ok';
exception when unique_violation then return 'dup';
end; $$ language plpgsql;
select safeadd(5);
select count(*)::int as present from uu where id = 5;

-- @case: plpgsql FOR-over-query with rec.col
-- @expect: [{"r":60}]
create table q (id int, v int);
insert into q values (1,10),(2,20),(3,30);
create function sumv() returns int as $$
declare r record; s int := 0;
begin for r in select * from q loop s := s + r.v; end loop; return s; end;
$$ language plpgsql;
select sumv() as r;

-- @case: plpgsql RETURN QUERY (set-returning)
-- @expect: [{"oid":2,"ov":20},{"oid":3,"ov":30}]
-- (out columns are named oid/ov to avoid postgres' ambiguity with src.id / src.v)
create table src (id int, v int);
insert into src values (1,10),(2,20),(3,30);
create function bigrows(threshold int) returns table(oid int, ov int) as $$
begin return query select id, v from src where v > threshold order by id; end;
$$ language plpgsql;
select oid, ov from bigrows(15) order by oid;

-- @case: RETURNS SETOF scalar
-- @expect: [{"nums":1},{"nums":2},{"nums":3}]
create function nums(n int) returns setof int as $$
declare i int;
begin for i in 1..n loop return next i; end loop; end;
$$ language plpgsql;
select * from nums(3);
