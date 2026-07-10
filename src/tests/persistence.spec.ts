import { describe, it, expect } from 'bun:test';

import { newDb } from '../db';

/** round-trip a db through JSON serialize -> deserialize into a fresh db */
function roundTrip(setup: (db: ReturnType<typeof newDb>) => void) {
    const db = newDb();
    setup(db);
    const json = JSON.stringify(db.serialize());
    const restored = newDb();
    restored.deserialize(JSON.parse(json));
    return restored;
}

describe('durable persistence (serialize / deserialize)', () => {

    it('round-trips tables and rows', () => {
        const db = roundTrip(d => d.public.none(`
            create table t (id int primary key, name text);
            insert into t values (1, 'a'), (2, 'b');
        `));
        expect(db.public.many(`select * from t order by id`))
            .toEqual([{ id: 1, name: 'a' }, { id: 2, name: 'b' }]);
    });

    it('preserves dates, jsonb, arrays and nulls', () => {
        const db = roundTrip(d => d.public.none(`
            create table t (id int, ts timestamp, meta jsonb, tags text[]);
            insert into t values (1, timestamp '2020-01-02 03:04:05', '{"a":1}', array['x','y']),
                                 (2, null, null, null);
        `));
        const rows = db.public.many(`select id, to_char(ts, 'YYYY-MM-DD HH24:MI:SS') as ts, meta, tags from t order by id`);
        expect(rows).toEqual([
            { id: 1, ts: '2020-01-02 03:04:05', meta: { a: 1 }, tags: ['x', 'y'] },
            { id: 2, ts: null, meta: null, tags: null },
        ]);
    });

    it('replays enums, foreign keys and views', () => {
        const db = roundTrip(d => d.public.none(`
            create type mood as enum ('happy', 'sad');
            create table people (id serial primary key, m mood);
            insert into people (m) values ('happy'), ('sad');
            create table notes (id int, person_id int references people(id));
            insert into notes values (1, 1);
            create view happy as select id from people where m = 'happy';
        `));
        expect(db.public.many(`select id, m from people order by id`))
            .toEqual([{ id: 1, m: 'happy' }, { id: 2, m: 'sad' }]);
        expect(db.public.many(`select * from happy`)).toEqual([{ id: 1 }]);
        // FK still enforced after restore
        expect(() => db.public.none(`insert into notes values (2, 999)`)).toThrow();
    });

    it('advances serial counters past restored ids', () => {
        const db = roundTrip(d => d.public.none(`
            create table t (id serial primary key, v text);
            insert into t (v) values ('a'), ('b'), ('c');
        `));
        db.public.none(`insert into t (v) values ('d')`);
        expect(db.public.many(`select id from t order by id`))
            .toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
    });

    it('does not re-fire triggers when loading data', () => {
        const db = roundTrip(d => d.public.none(`
            create table t (id int, doubled int);
            create table audit (n int);
            create function trg() returns trigger as $$
              begin insert into audit(n) values (new.id); return new; end;
            $$ language plpgsql;
            create trigger tr after insert on t for each row execute function trg();
            insert into t values (1, 10), (2, 20);
        `));
        // the two original inserts logged 2 audit rows; restoring must not add more
        expect(db.public.one(`select count(*)::int as c from audit`).c).toEqual(2);
        expect(db.public.many(`select * from t order by id`))
            .toEqual([{ id: 1, doubled: 10 }, { id: 2, doubled: 20 }]);
    });

    it('handles multiple schemas', () => {
        const db = roundTrip(d => d.public.none(`
            create schema app;
            create table app.t (id int, v text);
            insert into app.t values (1, 'x');
        `));
        expect(db.public.many(`select * from app.t`)).toEqual([{ id: 1, v: 'x' }]);
    });

    it('serializes an empty database to an empty snapshot', () => {
        expect(newDb().serialize()).toEqual({ pgMemPersistence: 1, ddl: [], data: [] });
    });

    it('rejects an invalid snapshot', () => {
        expect(() => newDb().deserialize({} as any)).toThrow(/Invalid pg-mem snapshot/);
    });
});
