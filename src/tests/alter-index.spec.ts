import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('ALTER INDEX & TABLESPACE', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        none(`create table t (id int, email text);
              insert into t values (1, 'a@x.com'), (2, 'b@x.com')`);
    });

    it('renames an index; the old name is gone and the new one drops', () => {
        none(`create index my_idx on t (email)`);
        none(`alter index my_idx rename to email_idx`);
        expectQueryError(() => none(`drop index my_idx`), /does not exist|not belong/);
        none(`drop index email_idx`);
    });

    it('keeps a unique index enforcing after rename', () => {
        none(`create unique index u on t (email)`);
        none(`alter index u rename to u2`);
        expectQueryError(() => none(`insert into t values (3, 'a@x.com')`), /unique|duplicate/i);
    });

    it('ALTER INDEX IF EXISTS on a missing index is a no-op', () => {
        none(`alter index if exists nope rename to whatever`);
    });

    it('SET TABLESPACE on an index is accepted (no-op)', () => {
        none(`create index i on t (id)`);
        none(`alter index i set tablespace fast`);
    });

    it('TABLESPACE clauses on CREATE TABLE / INDEX are accepted (no-op)', () => {
        none(`create table t2 (id int) tablespace fast`);
        none(`create index i2 on t2 (id) tablespace fast`);
        none(`insert into t2 values (1)`);
        expect(db.public.many(`select id from t2`).map(r => r.id)).toEqual([1]);
    });
});
