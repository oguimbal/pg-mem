import { describe, it, beforeEach, expect } from 'bun:test';
import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { delay } from '../utils';
import { createAdvancedProxy, createAdvancedServer, createSimpleProxy, DbRawCommand, IAdvancedProxySession, IAdvanceServerSession, IResponseWriter, ISimpleProxySession, ProxyParties } from 'pg-server';
import { Socket } from 'net';
import connect from 'postgres'
import { DbRawResponse } from 'pg-server/protocol/response-parser';


// TO TEST AGAINST REAL POSTGRES:
//  1) spin up a DB
//     docker run --name test-postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres
//  2) connect to it & create table
//  3) in node_modules/postgres/src/connection.js => handle() add a console.log(' => ', fnName);
//  4) just paste this to use the real postgres:
// const sql = connect('postgres://postgres:mysecretpassword@localhost:5432/postgres', {
//     ssl: false,
// } as any) as any;

describe('Postgres.js', () => {
    let db: IMemoryDb;
    type Tag = (strings: TemplateStringsArray, ...values: any[]) => Promise<any[]>
    let sql: Tag & { begin: <T>(handler: (sql: Tag) => Promise<T>) => Promise<T> };

    beforeEach(() => {
        db = newDb();
        db.public.query(`
            create table users (id serial primary key, name text, is_ok boolean, data jsonb);
            insert into users (name, is_ok, data) values
                ('Alice', true, '{"gender":"female"}'),
                ('Bob', false, null),
                ('Anon', null, null);
            `);
        const origTag = db.adapters.createPostgresJsTag();
        const NOK = Symbol();
        const wrapTag = (tag: any) => {
            return (strings: TemplateStringsArray, ...values: any[]) => {
                return Promise.race([
                    delay(500000).then(() => NOK),
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
        };
        sql = wrapTag(origTag) as any;
        (sql as any).begin = async (handler: any) => {
            return origTag.begin((newTag: any) => {
                return handler(wrapTag(newTag));
            });
        };
    })

    it('query though postgres.js without arguments', async () => {
        const results = await sql`select * from users`;
        expect(results).toEqual([
            { id: 1, name: 'Alice', is_ok: true, data: { gender: 'female' } },
            { id: 2, name: 'Bob', is_ok: false, data: null },
            { id: 3, name: 'Anon', is_ok: null, data: null },
        ]);
    });

    it('query though postgres.js with arguments', async () => {
        const arg = 'Alice';
        const results = await sql`select * from users where name = ${arg}`;
        expect(results).toEqual([
            { id: 1, name: 'Alice', is_ok: true, data: { gender: 'female' } },
        ]);
    });

    it('can execute insert', async () => {
        const arg = 'Charlie';
        const results = await sql`insert into users (name) values (${arg}) returning *`;
        expect(results).toEqual([
            { id: 4, name: 'Charlie', is_ok: null, data: null },
        ]);
    });

    it('can have a json argument in insert', async () => {
        const nm = 'Charlie';
        const dat = { gender: 'unknown' };
        const results = await sql`insert into users (name, data) values (${nm}, ${dat}) returning *`;
        expect(results).toEqual([
            { id: 4, name: 'Charlie', is_ok: null, data: { gender: 'unknown' } },
        ]);
    });


    it.only('can have a string argument casted to json', async () => {
        const nm = 'Charlie';
        // argument is a string, but that will be cased to JSON by the query
        const dat = '{"gender":"unknown"}';
        const results = await sql`insert into users (name, data) values (${nm}, ${dat}::jsonb) returning *`;
        expect(results).toEqual([
            { id: 4, name: 'Charlie', is_ok: null, data: { gender: 'unknown' } },
        ]);
    });

    it('can have a juson argument in query', async () => {
        const results = await sql`select * from users where data = ${{ gender: 'female' }}`;
        expect(results).toEqual([
            { id: 1, name: 'Alice', is_ok: true, data: { gender: 'female' } }
        ]);
    })


    it('can have a boolean argument', async () => {
        const results = await sql`insert into users (name, is_ok) values (${'A'}, ${true}), (${'B'}, ${false}) returning *`;
        expect(results).toEqual([
            { id: 4, name: 'A', is_ok: true, data: null },
            { id: 5, name: 'B', is_ok: false, data: null },
        ]);
    });

    it('can have a null argument', async () => {
        const results = await sql`insert into users (name, is_ok) values (${'A'}, ${null}) returning *`;
        expect(results).toEqual([
            { id: 4, name: 'A', is_ok: null, data: null },
        ]);
    });

    //
    // it.only('manual transaction commit', async () => {
    //     const arg = 'Charlie';
    //     expect(db.getTable('users').find().length).toBe(3);
    //     await sql`begin`;
    //     const results = await sql`insert into users (name) values (${arg}) returning *`;
    //     expect(db.getTable('users').find().length).toBe(3); // should not be committed yet
    //     expect(results).toEqual([
    //         { id: 4, name: 'Charlie', is_ok: null, data: null },
    //     ]);
    //     await sql`commit`;
    //     expect(db.getTable('users').find().length).toBe(4);
    // })

    it('can execute a transaction', async () => {
        // const sql = connect('postgres://postgres:mysecretpassword@localhost:5432/postgres', {
        //     ssl: false,
        // } as any) as any;

        const arg = 'Charlie';
        expect(db.getTable('users').find().length).toBe(3);
        const results = await sql.begin(async (tx: any) => {
            const results = await tx`insert into users (name) values (${arg}) returning *`;
            return results;
        });
        expect([...results]).toEqual([
            { id: 4, name: 'Charlie', is_ok: null, data: null },
        ]);
        expect(db.getTable('users').find().length).toBe(4);
    })


    it('does not commit transaction when begin failed', async () => {
        expect(db.getTable('users').find().length).toBe(3);
        let thrown = false;
        try {
            await sql.begin(async tx => {
                await tx`insert into users (name) values (${'Charlie'}) returning *`;
                throw new Error('rollback');
            });
        } catch (e) {
            thrown = true;
        }
        expect(thrown).toBe(true);
        expect(db.getTable('users').find().length).toBe(3); // count should not change
    })
});
