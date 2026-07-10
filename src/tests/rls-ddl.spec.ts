import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { _IDb } from '../interfaces-private';
import { expectQueryError } from './test-utils';

// Slice B: policy DDL is parsed and stored on the table. Enforcement is slice C.

describe('RLS policy DDL & storage', () => {

    let db: _IDb;
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        none = db.public.none.bind(db.public);
        none(`create table docs (id int, owner text, body text)`);
    });

    const rls = () => (db.public.getTable('docs') as any).rls;

    it('ENABLE/DISABLE/FORCE row level security', () => {
        expect(rls().enabled).toBe(false);
        none(`alter table docs enable row level security`);
        expect(rls().enabled).toBe(true);
        none(`alter table docs force row level security`);
        expect(rls().forced).toBe(true);
        none(`alter table docs no force row level security`);
        expect(rls().forced).toBe(false);
        none(`alter table docs disable row level security`);
        expect(rls().enabled).toBe(false);
    });

    it('stores policy metadata with postgres defaults', () => {
        none(`create policy p1 on docs using (owner = current_user)`);
        const [p] = rls().policies;
        expect(p.name).toBe('p1');
        expect(p.permissive).toBe(true); // default PERMISSIVE
        expect(p.command).toBe('all');   // default ALL
        expect(p.roles).toEqual([]);     // default PUBLIC
        expect(p.using).toBeTruthy();
        expect(p.withCheck ?? null).toBeNull();
    });

    it('captures AS / FOR / TO / WITH CHECK', () => {
        none(`create policy p2 on docs as restrictive for insert to alice, bob with check (owner = current_user)`);
        const p = rls().policies.find((x: any) => x.name === 'p2');
        expect(p.permissive).toBe(false);
        expect(p.command).toBe('insert');
        expect(p.roles).toEqual(['alice', 'bob']);
        expect(p.withCheck).toBeTruthy();
    });

    it('drop policy removes it; duplicate & missing error', () => {
        none(`create policy p on docs using (true)`);
        expectQueryError(() => none(`create policy p on docs using (true)`), /policy "p" for table "docs" already exists/);
        none(`drop policy p on docs`);
        expect(rls().policies).toEqual([]);
        expectQueryError(() => none(`drop policy p on docs`), /policy "p" for table "docs" does not exist/);
        none(`drop policy if exists p on docs`); // no throw
    });
});
