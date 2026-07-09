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
