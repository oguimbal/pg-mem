import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('MERGE', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        many = db.public.many.bind(db.public);
        none(`create table target (id int primary key, v text);
              create table source (id int, v text, del boolean);
              insert into target values (1, 'a'), (2, 'b'), (3, 'c');
              insert into source values (1, 'A', false), (2, 'B', true), (4, 'D', false)`);
    });

    it('upserts (matched update, not-matched insert)', () => {
        none(`merge into target t using source s on t.id = s.id
               when matched then update set v = s.v
               when not matched then insert (id, v) values (s.id, s.v)`);
        expect(many(`select * from target order by id`))
            .toEqual([{ id: 1, v: 'A' }, { id: 2, v: 'B' }, { id: 3, v: 'c' }, { id: 4, v: 'D' }]);
    });

    it('applies conditional matched actions in order (AND delete, then update)', () => {
        none(`merge into target t using source s on t.id = s.id
               when matched and s.del then delete
               when matched then update set v = s.v
               when not matched then insert (id, v) values (s.id, s.v)`);
        expect(many(`select * from target order by id`))
            .toEqual([{ id: 1, v: 'A' }, { id: 3, v: 'c' }, { id: 4, v: 'D' }]);
    });

    it('does nothing for matched when DO NOTHING', () => {
        none(`merge into target t using source s on t.id = s.id
               when matched then do nothing`);
        expect(many(`select * from target order by id`))
            .toEqual([{ id: 1, v: 'a' }, { id: 2, v: 'b' }, { id: 3, v: 'c' }]);
    });

    it('reports the affected row count', () => {
        const r = db.public.query(`merge into target t using source s on t.id = s.id
                                    when matched then update set v = s.v`);
        expect(r.command).toEqual('MERGE');
        expect(r.rowCount).toEqual(2);
    });

    it('inserts with column DEFAULTs filled', () => {
        none(`create table t2 (id int primary key, v text default 'def');
               create table s2 (id int);
               insert into t2 values (1, 'a');
               insert into s2 values (1), (2)`);
        none(`merge into t2 t using s2 s on t.id = s.id
               when not matched then insert (id) values (s.id)`);
        expect(many(`select * from t2 order by id`))
            .toEqual([{ id: 1, v: 'a' }, { id: 2, v: 'def' }]);
    });

    it('merges from a subquery source', () => {
        none(`merge into target t using (select id, v from source where not del) s on t.id = s.id
               when matched then update set v = s.v
               when not matched then insert (id, v) values (s.id, s.v)`);
        expect(many(`select * from target order by id`))
            .toEqual([{ id: 1, v: 'A' }, { id: 2, v: 'b' }, { id: 3, v: 'c' }, { id: 4, v: 'D' }]);
    });
});
