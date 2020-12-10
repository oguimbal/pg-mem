/* istanbul ignore file */
import { IMemoryDb, ISubscription } from '../interfaces';
import { assert, expect } from 'chai';
import { BaseEntity, Connection } from 'typeorm';
import { _IDb } from '../interfaces-private';
import { newDb } from '../index';
import { Ctor } from '../utils';

export function preventSeqScan(db: IMemoryDb, table?: string): ISubscription {
    if (table) {
        return db.getTable(table).on('seq-scan', () => {
            assert.fail('Should have used index');
        });
    } else {
        return db.on('seq-scan', table => {
            assert.fail('Should have used index when requesting table ' + table);
        });
    }
}

export function preventCataJoin(db: IMemoryDb) {
    return db.on('catastrophic-join-optimization', () => {
        assert.fail('Should have used index when performing join');
    });
}

export function watchCataJoins(db: IMemoryDb) {
    let got = 0;
    db.on('catastrophic-join-optimization', () => {
        got++;
    });
    return {
        check() {
            expect(got).to.equal(0, 'Should have used index when performing join');
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