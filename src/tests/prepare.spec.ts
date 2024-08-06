import { describe, it, beforeEach, expect } from 'bun:test';
import { newDb } from '../db';
import { DataType, IMemoryDb } from '../interfaces';
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

    it('describes a bool parameter in in query', () => {
        const prepared = db.public.prepare(`select $1 = true`);
        const { parameters } = prepared.describe();
        expect(parameters.map(x => x.type)).toEqual([DataType.bool]);
    });



    it('describes a bool parameter in in insert', () => {
        db.public.query(`create table test (a bool)`);
        const prepared = db.public.prepare(`insert into test values ($1)`);
        const { parameters } = prepared.describe();
        expect(parameters.map(x => x.type)).toEqual([DataType.bool]);
    });


    it('describes a number parameter in query', () => {
        const prepared = db.public.prepare(`select 42+$1`);
        const { parameters } = prepared.describe();
        expect(parameters.map(x => x.type)).toEqual([DataType.float]);
    });

    it('describes a number parameter in insert', () => {
        db.public.query(`create table test (a float)`);
        const prepared = db.public.prepare(`insert into test values ($1)`);
        const { parameters } = prepared.describe();
        expect(parameters.map(x => x.type)).toEqual([DataType.float]);
    });

    it('describes a jsonb parameter in query', () => {
        db.public.registerFunction({
            name: 'jsonb_array_length',
            args: [DataType.jsonb],
            returns: DataType.integer,
            implementation: a => a.length,
        });
        const prepared = db.public.prepare(`select jsonb_array_length($1)`);
        const { parameters } = prepared.describe();
        expect(parameters.map(x => x.type)).toEqual([DataType.jsonb]);
    });

    it('describes a jsonb parameter in insert', () => {
        db.public.query(`create table test (a jsonb)`);
        const prepared = db.public.prepare(`insert into test values ($1)`);
        const { parameters } = prepared.describe();
        expect(parameters.map(x => x.type)).toEqual([DataType.jsonb]);
    });



    it('describes a string parameter that will be casted to jsonb in query', () => {
        db.public.registerFunction({
            name: 'jsonb_array_length',
            args: [DataType.jsonb],
            returns: DataType.integer,
            implementation: a => a.length,
        });
        const prepared = db.public.prepare(`select jsonb_array_length($1::jsonb) len`);
        const { parameters } = prepared.describe();
        expect(parameters.map(x => x.type)).toEqual([DataType.jsonb]);

        // execute & check result
        const { rows } = prepared.bind(['[{"a":1}]']).executeAll();
        expect(rows).toEqual([{ len: 1 }]);
    });

    it('describes a string parameter that will be casted to jsonb in insert', () => {
        db.public.query(`create table test (a jsonb)`);
        const prepared = db.public.prepare(`insert into test values ($1::jsonb)`);
        const { parameters } = prepared.describe();
        expect(parameters.map(x => x.type)).toEqual([DataType.jsonb]);

        // execute & check result
        prepared.bind(['{"a":1}']).executeAll();
        expect(db.public.many(`select * from test`)).toEqual([{ a: { a: 1 } }]);
    });

    it('can execute with arguments', () => {
        const { rows } = db.public
            .prepare(`select id,name from users where name = $1`)
            .bind(['Alice'])
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