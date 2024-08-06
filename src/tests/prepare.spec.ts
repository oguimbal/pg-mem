import { describe, it, beforeEach, expect } from 'bun:test';
import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';


describe('Prepared statements', () => {
    let db: IMemoryDb;
    beforeEach(() => {
        db = newDb();
        db.public.query(`
            create table users (id serial primary key, name text, is_ok boolean, data jsonb);
            insert into users (name, is_ok, data) values
                ('Alice', true, '{"gender":"female"}'),
                ('Bob', false, null),
                ('Anon', null, null);
            `);
    });

    it('can prepare without arguments', () => {
        db.public.prepare(`select * from users`);
    });

    it('can prepare with arguments', () => {
        db.public.prepare(`select * from users where name = $1`);
    });


    it('can execute with arguments', () => {
        const { rows } = db.public
            .prepare(`select id,name from users where name = $1`)
            .bind('Alice')
            .executeAll();
        expect(rows).toEqual([{ id: 1, name: 'Alice' }]);
    });

    it('invalid sql', () => {
        expectQueryError(() => db.public.prepare(`selct * from`));
    });


    it('valid sql, but non existing', () => {
        expectQueryError(() => db.public.prepare(`select * from indexistant`).bind(), /relation "indexistant" does not exist/);
    });

    it('does not have any effect before execution', () => {
        expect(db.public.many(`select * from users`)).toHaveLength(3);
        const prepared = db.public.prepare(`truncate users`);
        expect(db.public.many(`select * from users`)).toHaveLength(3);
        const bound = prepared.bind();
        expect(db.public.many(`select * from users`)).toHaveLength(3);
        bound.executeAll();
        expect(db.public.many(`select * from users`)).toHaveLength(0);
    });
})