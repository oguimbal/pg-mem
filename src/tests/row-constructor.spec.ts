import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('ROW(...) constructor', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    // nb: pg-mem represents an anonymous record as an object keyed f1, f2, ...; the
    // postgres `(1,2)` record *text* rendering and field access belong to the (not yet
    // implemented) composite-type work.
    it('builds a record from literals', () => {
        expect(one(`select row(1, 2) as r`).r).toEqual({ f1: 1, f2: 2 });
    });

    it('builds a record of mixed types', () => {
        expect(one(`select row(1, 'a', true) as r`).r).toEqual({ f1: 1, f2: 'a', f3: true });
    });

    it('builds a record from columns', () => {
        none(`create table t (a int, b text);
               insert into t values (1, 'x'), (2, 'y')`);
        expect(many(`select row(a, b) as r from t order by a`).map(x => x.r))
            .toEqual([{ f1: 1, f2: 'x' }, { f1: 2, f2: 'y' }]);
    });

    it('feeds a record-typed function (e.g. a user row_to_json)', () => {
        db.public.registerFunction({
            name: 'row_to_json',
            args: [{ name: 'rec', type: db.public.getType('record' as any) as any }] as any,
            returns: db.public.getType('jsonb' as any) as any,
            implementation: (rec: any) => rec,
        } as any);
        expect(one(`select row_to_json(row(1, 2)) as j`).j).toEqual({ f1: 1, f2: 2 });
    });
});
