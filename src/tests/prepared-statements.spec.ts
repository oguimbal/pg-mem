import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('SQL-level PREPARE / EXECUTE / DEALLOCATE', () => {

    let db: IMemoryDb;
    let none: (str: string) => void;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        none = db.public.none.bind(db.public);
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
        none(`create table users (id int primary key, name text);
              insert into users values (1, 'alice'), (2, 'bob'), (3, 'carol')`);
    });

    it('prepares and executes a parameterized SELECT (indexed column)', () => {
        none(`prepare getu(int) as select name from users where id = $1`);
        expect(one(`execute getu(2)`).name).toEqual('bob');
        expect(one(`execute getu(3)`).name).toEqual('carol');
    });

    it('evaluates EXECUTE argument expressions', () => {
        none(`prepare getu(int) as select name from users where id = $1`);
        expect(one(`execute getu(1 + 1)`).name).toEqual('bob');
    });

    it('runs a prepared DML statement with multiple params', () => {
        none(`prepare addu(int, text) as insert into users values ($1, $2)`);
        none(`execute addu(4, 'dave')`);
        expect(many(`select name from users order by id`).map(r => r.name))
            .toEqual(['alice', 'bob', 'carol', 'dave']);
    });

    it('supports a no-argument prepared statement', () => {
        none(`prepare cnt as select count(*)::int as c from users`);
        expect(one(`execute cnt`).c).toEqual(3);
    });

    it('DEALLOCATE removes a single prepared statement', () => {
        none(`prepare getu(int) as select name from users where id = $1`);
        none(`deallocate getu`);
        expectQueryError(() => one(`execute getu(1)`), /does not exist/);
    });

    it('DEALLOCATE ALL clears every prepared statement', () => {
        none(`prepare a as select 1`);
        none(`prepare b as select 2`);
        none(`deallocate all`);
        expectQueryError(() => one(`execute a`), /does not exist/);
        expectQueryError(() => one(`execute b`), /does not exist/);
    });

    it('errors on EXECUTE of an unknown statement', () => {
        expectQueryError(() => none(`execute nope(1)`), /does not exist/);
    });

    it('errors on a duplicate PREPARE name', () => {
        none(`prepare dup as select 1`);
        expectQueryError(() => none(`prepare dup as select 2`), /already exists/);
    });

    it('[bugfix] a parameter compared to an indexed column no longer crashes at bind', () => {
        // regression: the index-seek fast path used to .get() the parameter at build time
        const rows = db.public.prepare(`select name from users where id = $1`).bind([2]).executeAll().rows;
        expect(rows).toEqual([{ name: 'bob' }]);
    });

    it('[bugfix] a scalar subquery compared to an indexed column works', () => {
        expect(one(`select name from users where id = (select min(id) from users)`).name)
            .toEqual('alice');
    });
});
