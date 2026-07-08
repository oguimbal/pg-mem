import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('Deferrable constraints', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('defers the FK check to commit (insert child before parent)', () => {
        none(`create table a (id int primary key);
               create table b (id int references a (id) deferrable initially deferred);
               begin;
               insert into b values (1);
               insert into a values (1);
               commit`);
        expect(many(`select id from b`).map(r => r.id)).toEqual([1]);
    });

    it('still errors at commit when the FK is never satisfied, and nothing persists', () => {
        expectQueryError(() => none(`create table a (id int primary key);
               create table b (id int references a (id) deferrable initially deferred);
               begin;
               insert into b values (99);
               commit`), /violates foreign key constraint/);
        expect(many(`select id from b`)).toEqual([]);
    });

    it('a non-deferrable FK still fails immediately', () => {
        expectQueryError(() => none(`create table a (id int primary key);
               create table b (id int references a (id));
               insert into b values (1)`), /violates foreign key constraint/);
    });
});
