import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('PL/pgSQL functions', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('runs a scalar function with a local variable and IF/ELSIF/ELSE', () => {
        none(`create function classify(n int) returns text as $$
            declare r text;
            begin
                if n > 0 then r := 'pos';
                elsif n < 0 then r := 'neg';
                else r := 'zero';
                end if;
                return r;
            end; $$ language plpgsql`);
        expect(one(`select classify(5) as v`).v).toEqual('pos');
        expect(one(`select classify(-2) as v`).v).toEqual('neg');
        expect(one(`select classify(0) as v`).v).toEqual('zero');
    });

    it('runs a FOR..LOOP with accumulation', () => {
        none(`create function sumto(n int) returns int as $$
            declare s int := 0; i int;
            begin for i in 1..n loop s := s + i; end loop; return s; end;
        $$ language plpgsql`);
        expect(one(`select sumto(10) as v`).v).toEqual(55);
        expect(one(`select sumto(100) as v`).v).toEqual(5050);
    });

    it('supports REVERSE and BY in a FOR range', () => {
        none(`create function countdown(n int) returns int as $$
            declare c int := 0; i int;
            begin for i in reverse n..1 loop c := c + 1; end loop; return c; end;
        $$ language plpgsql`);
        expect(one(`select countdown(5) as v`).v).toEqual(5);
        none(`create function evens(n int) returns int as $$
            declare c int := 0; i int;
            begin for i in 0..n by 2 loop c := c + 1; end loop; return c; end;
        $$ language plpgsql`);
        expect(one(`select evens(10) as v`).v).toEqual(6);
    });

    it('supports a WHILE loop with EXIT', () => {
        none(`create function firstpow2(n int) returns int as $$
            declare p int := 1;
            begin
                while true loop
                    if p >= n then exit; end if;
                    p := p * 2;
                end loop;
                return p;
            end; $$ language plpgsql`);
        expect(one(`select firstpow2(100) as v`).v).toEqual(128);
    });

    it('supports EXIT WHEN and CONTINUE WHEN', () => {
        none(`create function countodd(n int) returns int as $$
            declare c int := 0; i int;
            begin
                for i in 1..n loop
                    continue when i % 2 = 0;
                    exit when i > 100;
                    c := c + 1;
                end loop;
                return c;
            end; $$ language plpgsql`);
        expect(one(`select countodd(10) as v`).v).toEqual(5);
    });

    it('supports recursion', () => {
        none(`create function fact(n int) returns int as $$
            begin if n <= 1 then return 1; end if; return n * fact(n - 1); end;
        $$ language plpgsql`);
        expect(one(`select fact(5) as v`).v).toEqual(120);
    });

    it('supports mutual recursion', () => {
        none(`create function iseven(n int) returns boolean as $$
            begin if n = 0 then return true; end if; return isodd(n - 1); end;
        $$ language plpgsql`);
        none(`create function isodd(n int) returns boolean as $$
            begin if n = 0 then return false; end if; return iseven(n - 1); end;
        $$ language plpgsql`);
        expect(one(`select iseven(10) as v`).v).toEqual(true);
        expect(one(`select iseven(7) as v`).v).toEqual(false);
    });

    it('handles typed variables (numeric, text) and expressions', () => {
        none(`create function withtax(p numeric) returns numeric as $$
            declare rate numeric := 0.2;
            begin return p + p * rate; end;
        $$ language plpgsql`);
        // nb: pg-mem normalises numeric scale (120 vs postgres' 120.0)
        expect(one(`select withtax(100) as v`).v).toEqual('120');
    });

    it('concatenates text', () => {
        none(`create function greet(nm text) returns text as $$
            begin return 'hi ' || nm; end;
        $$ language plpgsql`);
        expect(one(`select greet('bob') as v`).v).toEqual('hi bob');
    });

    describe('embedded SQL', () => {
        beforeEach(() => {
            none(`create table t (id int, v int); insert into t values (1,10),(2,20),(3,30)`);
        });

        it('SELECT ... INTO a variable, with a parameter in WHERE', () => {
            none(`create function cntgt(threshold int) returns int as $$
                declare c int;
                begin select count(*)::int into c from t where v > threshold; return c; end;
            $$ language plpgsql`);
            expect(one(`select cntgt(15) as r`).r).toEqual(2);
            expect(one(`select cntgt(5) as r`).r).toEqual(3);
        });

        it('sets FOUND after a SELECT INTO', () => {
            none(`create function has_id(pid int) returns boolean as $$
                declare x int;
                begin select id into x from t where id = pid; return found; end;
            $$ language plpgsql`);
            expect(one(`select has_id(2) as r`).r).toEqual(true);
            expect(one(`select has_id(99) as r`).r).toEqual(false);
        });

        it('runs an embedded INSERT (void function)', () => {
            none(`create function addrow(pid int, pv int) returns void as $$
                begin insert into t(id, v) values (pid, pv); end;
            $$ language plpgsql`);
            none(`select addrow(4, 40)`);
            expect(many(`select id from t order by id`).map(r => r.id)).toEqual([1, 2, 3, 4]);
        });

        it('runs UPDATE inside a loop', () => {
            none(`create function bumpall() returns void as $$
                declare i int;
                begin for i in 1..3 loop update t set v = v + 1 where id = i; end loop; end;
            $$ language plpgsql`);
            none(`select bumpall()`);
            expect(many(`select v from t order by id`).map(r => r.v)).toEqual([11, 21, 31]);
        });

        it('runs dynamic EXECUTE', () => {
            none(`create function dyn(sql text) returns void as $$
                begin execute sql; end;
            $$ language plpgsql`);
            none(`select dyn('insert into t(id,v) values (9, 90)')`);
            expect(one(`select v from t where id = 9`).v).toEqual(90);
        });

        it('PERFORM runs a query and sets FOUND', () => {
            none(`create function any_big() returns boolean as $$
                begin perform 1 from t where v >= 100; return found; end;
            $$ language plpgsql`);
            expect(one(`select any_big() as r`).r).toEqual(false);
            none(`insert into t(id, v) values (5, 500)`);
            expect(one(`select any_big() as r`).r).toEqual(true);
        });
    });
});
