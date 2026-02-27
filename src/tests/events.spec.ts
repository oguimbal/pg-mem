import { describe, it, beforeEach, expect, afterEach } from 'bun:test';
import { DataType, IMemoryDb, ISchema, QueryResult } from '../interfaces';
import { _IDb, _ISchema } from '../interfaces-private';
import { newDb } from '../db';
import { expectQueryError } from './test-utils';


describe('Events', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let query: (str: string) => QueryResult;
    let queryEventSQL: string;
    let queryFailedEventSQL: string;
    let schemaChangeEventFired: boolean;
    let createExtensionEventResult: any;

    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        query = db.public.query.bind(db.public);

        db.on('query', (sql: string) => {
            queryEventSQL = sql;
        });

        db.on('query-failed', (sql: string) => {
            queryFailedEventSQL = sql;
        });

        db.on('schema-change', (_: IMemoryDb) => {
            schemaChangeEventFired = true;
        });

        db.on('create-extension', (extension: { name: string }) => {
            createExtensionEventResult = extension;
        });
    });

    afterEach(() => {
        queryEventSQL = '';
        queryFailedEventSQL = '';
        schemaChangeEventFired = false;
        createExtensionEventResult = undefined;
    });

    it('raises "query" event on a succesful query', () => {
        many(`select;`);
        expect(queryEventSQL === '', '"query" event should have been fired').toBeFalse();
        expect(queryEventSQL).toEqual('select;');
    });

    it('raises "query-failed" event on a failed query', () => {
        expectQueryError(() => none(`select * from non_existing_table;`));
        expect(queryFailedEventSQL === '', '"query-failed" event should have been fired').toBeFalse();
        expect(queryFailedEventSQL).toEqual('select * from non_existing_table;');
    });

    it('raises "query-failed" event on unique key constraint violation', () => {
        many(`create table test(id integer primary key);`);
        expectQueryError(() => none(`insert into test values (1), (1);`));
        expect(queryFailedEventSQL === '', '"query-failed" event should have been fired').toBeFalse();
        expect(queryFailedEventSQL).toEqual('insert into test values (1), (1);');
    });

    it('raises "query-failed" event on foreign key constraint violation', () => {
        many(`create table test(id integer primary key);
              create table test2(id integer primary key, test_id integer references test(id));`);
        expectQueryError(() => none(`insert into test2 values (1, 1);`));
        expect(queryFailedEventSQL === '', '"query-failed" event should have been fired').toBeFalse();
        expect(queryFailedEventSQL).toEqual('insert into test2 values (1, 1);');
    });

    it('raises "query-failed" event on not-null constraint violation', () => {
        many(`create table test(id integer primary key, name text not null);`);
        expectQueryError(() => none(`insert into test values (1, null);`));
        expect(queryFailedEventSQL === '', '"query-failed" event should have been fired').toBeFalse();
        expect(queryFailedEventSQL).toEqual('insert into test values (1, null);');
    });

    it('raises "schema-change" event on schema change', () => {
        many(`create table test(id integer primary key);`);
        expect(schemaChangeEventFired, '"schema-change" event should have been fired').toBeTrue();
    });

    it('does not raise "schema-change" event on non-changing-schema query', () => {
        many(`select;`);
        expect(schemaChangeEventFired === false, '"schema-change" event should not have been fired').toBeTrue();
    });

    it('raises "extension-create" event on extension creation', () => {
        db.registerExtension('ext', s => s.registerFunction({
            name: 'say_hello',
            args: [DataType.text],
            returns: DataType.text,
            implementation: x => 'hello ' + x,
        }));

        many(`create extension ext;`);
        expect(createExtensionEventResult === undefined, '"create-extension" event should have been fired').toBeFalse();
        expect(createExtensionEventResult.name, '"create-extension" event should have been fired').toEqual('ext');
    });
});