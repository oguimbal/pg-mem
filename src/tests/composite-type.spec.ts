import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('composite types', () => {

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

    it('stores and reads back a composite column', () => {
        none(`create type pt as (x int, y int);
               create table shapes (id int, center pt);
               insert into shapes values (1, row(3, 4)::pt), (2, row(10, 20)::pt)`);
        expect(one(`select center from shapes where id = 1`).center).toEqual({ x: 3, y: 4 });
    });

    it('accesses fields with (expr).field', () => {
        none(`create type pt as (x int, y int);
               create table shapes (id int, center pt);
               insert into shapes values (1, row(3, 4)::pt), (2, row(10, 20)::pt)`);
        expect(many(`select id, (center).x as cx, (center).y as cy from shapes order by id`))
            .toEqual([{ id: 1, cx: 3, cy: 4 }, { id: 2, cx: 10, cy: 20 }]);
    });

    it('filters on a composite field', () => {
        none(`create type pt as (x int, y int);
               create table shapes (id int, center pt);
               insert into shapes values (1, row(3, 4)::pt), (2, row(10, 20)::pt)`);
        expect(many(`select id from shapes where (center).x > 5 order by id`))
            .toEqual([{ id: 2 }]);
    });

    it('supports text fields', () => {
        none(`create type addr as (street text, city text);
               create table people (id int, home addr);
               insert into people values (1, row('1 Main', 'Springfield')::addr)`);
        expect(one(`select (home).city as c from people`).c).toEqual('Springfield');
    });

    it('returns a composite from a plpgsql function', () => {
        none(`create type pt as (x int, y int);
               create function mk() returns pt as $$ begin return row(7, 8)::pt; end; $$ language plpgsql`);
        expect(one(`select (mk()).x as x, (mk()).y as y`)).toEqual({ x: 7, y: 8 });
    });

    it('drops and recreates a type', () => {
        none(`create type pt as (x int, y int); drop type pt; create type pt as (a text)`);
        // no throw
    });

    it('rejects duplicate type names', () => {
        none(`create type pt as (x int, y int)`);
        expectQueryError(() => none(`create type pt as (a int)`), /already exists/);
    });
});
