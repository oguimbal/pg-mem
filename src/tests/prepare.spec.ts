import { describe, it, beforeEach, expect } from 'bun:test';
import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { delay } from '../utils';
import { expectQueryError } from './test-utils';


describe('Prepared statements', () => {
    let db: IMemoryDb;
    let sql: (strings: TemplateStringsArray, ...values: any[]) => Promise<any[]>;

    beforeEach(() => {
        db = newDb();
        db.public.query(`
            create table users (id serial primary key, name text, is_ok boolean, data jsonb);
            insert into users (name, is_ok, data) values
                ('Alice', true, '{"gender":"female"}'),
                ('Bob', false, null),
                ('Anon', null, null);
            `);
        const tag = db.adapters.createPostgresJsTag();
        const NOK = Symbol();
        sql = (strings: TemplateStringsArray, ...values: any[]) => {
            return Promise.race([
                delay(500).then(() => NOK),
                tag(strings, ...values),
            ]).then((res) => {
                if (res === NOK) {
                    expect('Adapter has timed out').toBe('');
                }
                // just remove other properties added by postgres.js:
                // count,  state,  command,  columns, statement
                return [...res];
            });
        }
    })

    it('query though postgres.js', async () => {
        const results = await sql`select * from users`;
        expect(results).toEqual([
            { id: 1, name: 'Alice', is_ok: true, data: { gender: 'female' } },
            { id: 2, name: 'Bob', is_ok: false, data: null },
            { id: 3, name: 'Anon', is_ok: null, data: null },
        ]);
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