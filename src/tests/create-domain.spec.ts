import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('CREATE DOMAIN', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    it('enforces a CHECK on insert', () => {
        none(`create domain posint as int check (value > 0);
               create table t (id int, qty posint);
               insert into t values (1, 5)`);
        expect(one(`select qty from t`).qty).toEqual(5);
        expectQueryError(() => none(`insert into t values (2, -3)`), /violates check constraint/);
    });

    it('enforces a CHECK on update and on explicit cast', () => {
        none(`create domain posint as int check (value > 0);
               create table t (id int, qty posint);
               insert into t values (1, 5)`);
        expectQueryError(() => none(`update t set qty = -1 where id = 1`), /violates check constraint/);
        expect(one(`select 7::posint as v`).v).toEqual(7);
        expectQueryError(() => one(`select (-1)::posint`), /violates check constraint/);
    });

    it('a domain value behaves as its base type', () => {
        none(`create domain posint as int check (value > 0);
               create table t (id int, qty posint);
               insert into t values (1, 5)`);
        expect(one(`select qty + 10 as r from t where id = 1`).r).toEqual(15);
    });

    it('enforces NOT NULL', () => {
        none(`create domain nzt as text not null;
               create table t (v nzt)`);
        expectQueryError(() => none(`insert into t values (null)`), /does not allow null/);
        none(`insert into t values ('hi')`);
        expect(one(`select v from t`).v).toEqual('hi');
    });

    it('supports a named check constraint and DEFAULT clause parsing', () => {
        none(`create domain score as int constraint score_range check (value >= 0 and value <= 100) default 0;
               create table t (s score)`);
        expectQueryError(() => none(`insert into t values (200)`), /score_range/);
        none(`insert into t values (50)`);
        expect(one(`select s from t`).s).toEqual(50);
    });
});
