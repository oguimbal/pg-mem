/* istanbul ignore file */
import { describe, it, expect } from 'bun:test';
import { IMemoryDb, ISubscription, QueryError } from '../interfaces';

import { BaseEntity, Connection } from 'typeorm';
import { _IDb } from '../interfaces-private';
import { newDb } from '../index';
import { Ctor } from '../utils';

export function preventSeqScan(db: IMemoryDb, table?: string): ISubscription {
    if (table) {
        return db.getTable(table).on('seq-scan', () => {
            expect('Should have used index').toBe('');
        });
    } else {
        return db.on('seq-scan', table => {
            expect('Should have used index when requesting table ' + table).toBe('');
        });
    }
}

export function preventCataJoin(db: IMemoryDb) {
    return db.on('catastrophic-join-optimization', () => {
        expect('Should have used index when performing join').toBe('');
    });
}

export function watchCataJoins(db: IMemoryDb) {
    let got = 0;
    db.on('catastrophic-join-optimization', () => {
        got++;
    });
    return {
        check() {
            expect(got).toBe(0) // 'Should have used index when performing join'
        }
    }
}


interface TypeOrmTest {
    db: Connection;
    mem: _IDb;
    many: (sql: string) => any[];
    one: (sql: string) => any;
    none: (sql: string) => void;
}

export type TypeormSetup = ((mem: Omit<TypeOrmTest, 'db'>) => any) | null;

export async function typeOrm(title: string
    , entities: () => Ctor<BaseEntity>[]
    , setup: TypeormSetup
    , fn: (data: TypeOrmTest) => Promise<any>) {
    it(title, async () => {
        const mem = newDb({
            autoCreateForeignKeyIndices: true,
        }) as _IDb;
        const many = mem.public.many.bind(mem.public);
        const none = mem.public.none.bind(mem.public);
        const one = mem.public.one.bind(mem.public);
        setup?.({ mem, many, none, one });

        const db: Connection = await mem.adapters.createTypeormConnection({
            type: 'postgres',
            entities: entities(),
        });
        try {
            await db.synchronize();
            await fn({ db, mem, many, none, one });
        } finally {
            await db.close()
        }
    });
}

export async function expectSingle(query: string, value: any, name?: string) {
    it(name ?? query, () => {
        const db = newDb();
        const q = db.public.many(query);
        expect(q.length).toBe(1)// 'Was only expecting one result'
        const keys = Object.keys(q[0]);
        expect(keys.length).toBe(1)// 'Was only expecting one column'
        expect(q[0][keys[0]]).toEqual(value);
    })
}

export function expectQueryError(fn: () => any, opts?: { code?: string, message?: string | RegExp } | RegExp) {
    if (opts instanceof RegExp) {
        opts = { message: opts };
    }
    try {
        fn();
    } catch (e: any) {
        expect(e).toBeInstanceOf(QueryError);
        if (opts?.code) {
            expect(e.data.code).toBe(opts.code);
        }
        if (opts?.message) {
            if (typeof opts.message === 'string') {
                expect(e.message).toContain(opts.message);
            } else {
                expect(e.message).toMatch(opts.message);
            }
        }
        return;
    }
    throw new Error('Expected to throw');
}