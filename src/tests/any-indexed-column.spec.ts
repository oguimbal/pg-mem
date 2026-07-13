import { describe, it, expect } from 'bun:test';
import { newDb } from '../db';

// Regression for #338: `col = ANY(array)` on an indexed column was optimized into a
// scalar index equality against the whole array, silently matching no rows. These
// cases exercise the index path (primary key); the seq-scan path was already correct.
describe('= ANY(array) on an indexed column', () => {

    function seeded() {
        const db = newDb();
        db.public.none(`create table tbl (id text primary key);
                        insert into tbl values ('A'), ('B'), ('C');`);
        return db;
    }

    it('matches rows via ARRAY[] on a primary-key column', () => {
        expect(seeded().public.many(`select * from tbl where id = any(array['A', 'C'])`))
            .toEqual([{ id: 'A' }, { id: 'C' }]);
    });

    it('matches rows via a string array literal on a primary-key column', () => {
        expect(seeded().public.many(`select * from tbl where id = any('{A,C}')`))
            .toEqual([{ id: 'A' }, { id: 'C' }]);
    });
});
