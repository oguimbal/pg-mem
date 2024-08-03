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
            create table users (id serial primary key, name text);
            insert into users (name) values ('Alice');
            insert into users (name) values ('Bob');
            `);
        const tag = db.adapters.createPostgresJsTag();
        const NOK = Symbol();
        sql = (strings: TemplateStringsArray, ...values: any[]) => {
            return Promise.race([
                delay(50).then(() => NOK),
                tag(strings, ...values),
            ]).then((res) => {
                if (res === NOK) {
                    expect('Adapter has timed out').toBe('');
                }
                return res;
            });
        }
    })

    it.skip('query though postgres.js', async () => {
        const results = await sql`select * from users`;
        expect(results).toEqual([
            { id: 1, name: 'Alice' },
            { id: 2, name: 'Bob' }
        ]);
    })

    it('can prepare without arguments', () => {
        db.public.prepare(`select * from users`);
    });

    it.only('can prepare with arguments', () => {
        db.public.prepare(`select * from users where name = $1`);
    });


    it('can execute with arguments', () => {
        const { rows } = db.public
            .prepare(`select * from users where name = $1`)
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
        expect(db.public.many(`select * from users`)).toHaveLength(2);
        const prepared = db.public.prepare(`truncate users`);
        expect(db.public.many(`select * from users`)).toHaveLength(2);
        const bound = prepared.bind();
        expect(db.public.many(`select * from users`)).toHaveLength(2);
        bound.executeAll();
        expect(db.public.many(`select * from users`)).toHaveLength(0);
    });
})