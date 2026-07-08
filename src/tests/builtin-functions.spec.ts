import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';
import { md5 } from '../utils/md5';

describe('Builtin functions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    describe('string', () => {
        it('length counts characters', () => {
            expect(one(`select length('hello') as v`).v).toBe(5);
            expect(one(`select char_length('josé') as v`).v).toBe(4);
            expect(one(`select length('') as v`).v).toBe(0);
        });

        it('length is null on null', () => {
            expect(one(`select length(null::text) as v`).v).toBeNull();
        });

        it('substr clamps like postgres', () => {
            expect(one(`select substr('alphabet', 3, 2) as v`).v).toBe('ph');
            expect(one(`select substr('alphabet', 0, 3) as v`).v).toBe('al');
            expect(one(`select substr('alphabet', -2, 4) as v`).v).toBe('a');
            expect(one(`select substr('alphabet', 3) as v`).v).toBe('phabet');
            expectQueryError(() => one(`select substr('alphabet', 3, -1)`), /negative substring length/);
        });

        it('replace replaces all occurrences', () => {
            expect(one(`select replace('abcdefabcdef', 'cd', 'XX') as v`).v).toBe('abXXefabXXef');
            expect(one(`select replace('abc', '', 'X') as v`).v).toBe('abc');
        });

        it('trim family', () => {
            expect(one(`select trim('  hi  ') as v`).v).toBe('hi');
            expect(one(`select btrim('xyxtrimyyx', 'xyz') as v`).v).toBe('trim');
            expect(one(`select ltrim('zzzytest', 'xyz') as v`).v).toBe('test');
            expect(one(`select rtrim('testxxzx', 'xyz') as v`).v).toBe('test');
        });

        it('lpad/rpad pad and truncate', () => {
            expect(one(`select lpad('hi', 5, 'xy') as v`).v).toBe('xyxhi');
            expect(one(`select rpad('hi', 5, 'xy') as v`).v).toBe('hixyx');
            expect(one(`select lpad('hi', 5) as v`).v).toBe('   hi');
            expect(one(`select lpad('hello', 3, 'xy') as v`).v).toBe('hel');
        });

        it('split_part supports negative fields', () => {
            expect(one(`select split_part('abc~@~def~@~ghi', '~@~', 2) as v`).v).toBe('def');
            expect(one(`select split_part('abc,def,ghi', ',', -1) as v`).v).toBe('ghi');
            expect(one(`select split_part('abc,def', ',', 10) as v`).v).toBe('');
            expectQueryError(() => one(`select split_part('a,b', ',', 0)`), /field position must not be zero/);
        });

        it('strpos', () => {
            expect(one(`select strpos('Thomas', 'om') as v`).v).toBe(3);
            expect(one(`select strpos('Thomas', 'xx') as v`).v).toBe(0);
        });

        it('initcap capitalizes words', () => {
            expect(one(`select initcap('hi THOMAS') as v`).v).toBe('Hi Thomas');
            expect(one(`select initcap('hello-world 2b') as v`).v).toBe('Hello-World 2b');
        });

        it('left/right support negative lengths', () => {
            expect(one(`select left('abcde', 2) as v`).v).toBe('ab');
            expect(one(`select left('abcde', -2) as v`).v).toBe('abc');
            expect(one(`select right('abcde', 2) as v`).v).toBe('de');
            expect(one(`select right('abcde', -2) as v`).v).toBe('cde');
        });

        it('misc string functions', () => {
            expect(one(`select reverse('abcde') as v`).v).toBe('edcba');
            expect(one(`select repeat('Pg', 3) as v`).v).toBe('PgPgPg');
            expect(one(`select repeat('Pg', -1) as v`).v).toBe('');
            expect(one(`select ascii('x') as v`).v).toBe(120);
            expect(one(`select chr(65) as v`).v).toBe('A');
            expect(one(`select translate('12345', '143', 'ax') as v`).v).toBe('a2x5');
            expect(one(`select starts_with('alphabet', 'alph') as v`).v).toBe(true);
            expect(one(`select octet_length('josé') as v`).v).toBe(5);
        });

        it('regexp_replace replaces first match by default', () => {
            expect(one(`select regexp_replace('Thomas', '.[mN]a.', 'M') as v`).v).toBe('ThM');
            expect(one(`select regexp_replace('foo foo', 'o', 'X', 'g') as v`).v).toBe('fXX fXX');
            expect(one(`select regexp_replace('abc', '(b)', '[\\1]') as v`).v).toBe('a[b]c');
        });

        it('format', () => {
            expect(one(`select format('Hello, %s', 'world') as v`).v).toBe('Hello, world');
            expect(one(`select format('%s and %s', 'a', 'b') as v`).v).toBe('a and b');
            expect(one(`select format('id %I', 'my col') as v`).v).toBe('id "my col"');
            expect(one(`select format('lit %L', 'o''brien') as v`).v).toBe(`lit 'o''brien'`);
            expect(one(`select format('%L', null) as v`).v).toBe('NULL');
            expect(one(`select format('100%%') as v`).v).toBe('100%');
            expect(one(`select format('%1$s %1$s', 'x') as v`).v).toBe('x x');
        });

        it('md5', () => {
            expect(md5('')).toBe('d41d8cd98f00b204e9800998ecf8427e');
            expect(md5('abc')).toBe('900150983cd24fb0d6963f7d28e17f72');
            expect(md5('hello world')).toBe('5eb63bbbe01eeed093cb22bb8f5acdc3');
            // 56+ chars exercises the two-block tail path
            expect(md5('12345678901234567890123456789012345678901234567890123456'))
                .toBe('49f193adce178490e34d1b3a4ec0064c');
            expect(one(`select md5('abc') as v`).v).toBe('900150983cd24fb0d6963f7d28e17f72');
        });
    });

    describe('math', () => {
        it('rounds half away from zero like pg numerics', () => {
            expect(one(`select round(2.5) as v`).v).toBe(3);
            expect(one(`select round(-2.5) as v`).v).toBe(-3);
            expect(one(`select round(42.4382, 2) as v`).v).toBe(42.44);
        });

        it('misc math functions', () => {
            expect(one(`select abs(-17.4) as v`).v).toBe(17.4);
            expect(one(`select ceil(-42.8) as v`).v).toBe(-42);
            expect(one(`select ceiling(48.2) as v`).v).toBe(49);
            expect(one(`select floor(-42.8) as v`).v).toBe(-43);
            expect(one(`select trunc(42.8) as v`).v).toBe(42);
            expect(one(`select trunc(-42.8) as v`).v).toBe(-42);
            expect(one(`select power(2, 10) as v`).v).toBe(1024);
            expect(one(`select sqrt(2) as v`).v).toBe(1.4142135623730951);
            expect(one(`select mod(9, 4) as v`).v).toBe(1);
            expect(one(`select mod(-9, 4) as v`).v).toBe(-1);
            expect(one(`select sign(-8.4) as v`).v).toBe(-1);
            expect(one(`select pi() as v`).v).toBe(Math.PI);
        });

        it('errors on invalid domains', () => {
            expectQueryError(() => one(`select sqrt(-1)`), /square root of a negative/);
            expectQueryError(() => one(`select ln(0)`), /nonpositive/);
            expectQueryError(() => one(`select mod(1, 0)`), /division by zero/);
        });
    });

    describe('date', () => {
        it('date_trunc', () => {
            expect(one(`select date_trunc('month', timestamp '2020-02-15 10:30:45') as v`).v)
                .toEqual(new Date(Date.UTC(2020, 1, 1)));
            expect(one(`select date_trunc('year', timestamp '2020-02-15 10:30:45') as v`).v)
                .toEqual(new Date(Date.UTC(2020, 0, 1)));
            expect(one(`select date_trunc('hour', timestamp '2020-02-15 10:30:45') as v`).v)
                .toEqual(new Date(Date.UTC(2020, 1, 15, 10)));
            // pg weeks start monday: 2020-02-15 is a saturday
            expect(one(`select date_trunc('week', timestamp '2020-02-15 10:30:45') as v`).v)
                .toEqual(new Date(Date.UTC(2020, 1, 10)));
            expectQueryError(() => one(`select date_trunc('nope', timestamp '2020-02-15')`), /unit/);
        });

        it('date_part', () => {
            expect(one(`select date_part('month', date '2020-02-15') as v`).v).toBe(2);
            expect(one(`select date_part('year', date '2020-02-15') as v`).v).toBe(2020);
            expect(one(`select date_part('dow', date '2020-02-15') as v`).v).toBe(6);
            expect(one(`select date_part('isodow', date '2020-02-16') as v`).v).toBe(7);
            expect(one(`select date_part('doy', date '2020-02-15') as v`).v).toBe(46);
            expect(one(`select date_part('epoch', timestamp '1970-01-01 00:01:00') as v`).v).toBe(60);
        });

        it('make_date', () => {
            expect(one(`select make_date(2020, 2, 15) as v`).v)
                .toEqual(new Date(Date.UTC(2020, 1, 15)));
        });

        it('age computes symbolic calendar differences', () => {
            // the canonical pg docs example
            expect(one(`select age(timestamp '2001-04-10', timestamp '1957-06-13') as v`).v)
                .toEqual({ years: 43, months: 9, days: 27 });
            expect(one(`select age(timestamp '1957-06-13', timestamp '2001-04-10') as v`).v)
                .toEqual({ years: -43, months: -9, days: -27 });
            expect(one(`select age(timestamp '2020-03-01 10:00:00', timestamp '2020-02-28 22:00:00') as v`).v)
                .toEqual({ days: 1, hours: 12 });
        });

        it('justify_interval family', () => {
            expect(one(`select justify_interval(interval '1 mon -1 hour') as v`).v)
                .toEqual({ days: 29, hours: 23 });
            expect(one(`select justify_hours(interval '27 hours') as v`).v)
                .toEqual({ days: 1, hours: 3 });
            expect(one(`select justify_days(interval '35 days') as v`).v)
                .toEqual({ months: 1, days: 5 });
        });
    });

    describe('nullif', () => {
        it('behaves like pg', () => {
            expect(one(`select nullif(5, 5) as v`).v).toBeNull();
            expect(one(`select nullif(5, 4) as v`).v).toBe(5);
            expect(one(`select nullif('a', 'b') as v`).v).toBe('a');
            expect(one(`select nullif(null::int, 1) as v`).v).toBeNull();
            // not strict: a null second argument does not nullify the first
            expect(one(`select nullif(1, null::int) as v`).v).toBe(1);
        });
    });
});
