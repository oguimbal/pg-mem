import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('partitioning', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('routes range-partitioned inserts and unions on the parent', () => {
        none(`create table events (id int, ts date) partition by range (ts);
               create table events_2020 partition of events for values from ('2020-01-01') to ('2021-01-01');
               create table events_2021 partition of events for values from ('2021-01-01') to ('2022-01-01');
               insert into events values (1, '2020-06-01'), (2, '2021-03-01'), (3, '2020-12-31')`);
        expect(many(`select id from events order by id`)).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
        expect(many(`select id from events_2020 order by id`)).toEqual([{ id: 1 }, { id: 3 }]);
        expect(many(`select id from events_2021 order by id`)).toEqual([{ id: 2 }]);
    });

    it('errors when no partition accepts a parent insert', () => {
        none(`create table events (id int, ts date) partition by range (ts);
               create table events_2020 partition of events for values from ('2020-01-01') to ('2021-01-01')`);
        expectQueryError(() => none(`insert into events values (9, '2019-01-01')`),
            /no partition of relation "events" found for row/);
    });

    it('routes list-partitioned inserts', () => {
        none(`create table items (id int, cat text) partition by list (cat);
               create table items_ab partition of items for values in ('a', 'b');
               create table items_c partition of items for values in ('c');
               insert into items values (1, 'a'), (2, 'c'), (3, 'b')`);
        expect(many(`select id from items_ab order by id`)).toEqual([{ id: 1 }, { id: 3 }]);
        expect(many(`select id from items_c order by id`)).toEqual([{ id: 2 }]);
    });

    it('sends unmatched rows to a DEFAULT partition', () => {
        none(`create table t (id int, cat text) partition by list (cat);
               create table t_ab partition of t for values in ('a', 'b');
               create table t_def partition of t default;
               insert into t values (1, 'a'), (2, 'z'), (3, 'b'), (4, 'q')`);
        expect(many(`select id from t_def order by id`)).toEqual([{ id: 2 }, { id: 4 }]);
        expect(many(`select id from t_ab order by id`)).toEqual([{ id: 1 }, { id: 3 }]);
    });

    it('enforces the bound on a direct child insert', () => {
        none(`create table items (id int, cat text) partition by list (cat);
               create table items_c partition of items for values in ('c')`);
        expectQueryError(() => none(`insert into items_c values (5, 'a')`),
            /violates partition constraint/);
    });

    it('supports a direct child insert within its bound', () => {
        none(`create table items (id int, cat text) partition by list (cat);
               create table items_c partition of items for values in ('c');
               insert into items_c values (5, 'c')`);
        expect(many(`select id from items order by id`)).toEqual([{ id: 5 }]);
    });
});
