import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('Savepoints', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        none(`create table t (id int)`);
    });

    const ids = () => many(`select id from t order by id`).map(r => r.id);

    it('rolls back to a savepoint, keeping earlier work', () => {
        none(`begin;
              insert into t values (1);
              savepoint sp1;
              insert into t values (2);
              rollback to savepoint sp1;
              commit;`);
        expect(ids()).toEqual([1]);
    });

    it('ROLLBACK TO without the SAVEPOINT keyword works', () => {
        none(`begin;
              insert into t values (1);
              savepoint sp1;
              insert into t values (2);
              rollback to sp1;
              insert into t values (3);
              commit;`);
        expect(ids()).toEqual([1, 3]);
    });

    it('a savepoint can be rolled back to more than once', () => {
        none(`begin;
              savepoint sp1;
              insert into t values (1);
              rollback to sp1;
              insert into t values (2);
              rollback to sp1;
              insert into t values (3);
              commit;`);
        expect(ids()).toEqual([3]);
    });

    it('release discards a savepoint but keeps its work', () => {
        none(`begin;
              insert into t values (1);
              savepoint sp1;
              insert into t values (2);
              release savepoint sp1;
              commit;`);
        expect(ids()).toEqual([1, 2]);
        // sp1 is gone now
        expectQueryError(() => none(`begin; rollback to sp1;`), /savepoint "sp1" does not exist/);
    });

    it('rolling back to a savepoint discards later savepoints', () => {
        none(`begin;
              savepoint sp1;
              insert into t values (1);
              savepoint sp2;
              insert into t values (2);
              rollback to sp1;`);
        expectQueryError(() => none(`rollback to sp2`), /savepoint "sp2" does not exist/);
        none(`rollback`);
    });

    it('errors on unknown savepoint', () => {
        expectQueryError(() => none(`begin; rollback to nope;`), /savepoint "nope" does not exist/);
        none(`rollback`);
        expectQueryError(() => none(`begin; release nope;`), /savepoint "nope" does not exist/);
        none(`rollback`);
    });
});
