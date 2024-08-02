import { newDb } from '../db';
import { kyselySample } from '../../samples/kysely/kysely';
import { describe, it, beforeEach, expect } from 'bun:test';
import type { Kysely } from "kysely";

describe('Kysely', () => {
    it('can perform sample', async () => {
        await kyselySample();
    });

    it('should use kysely config from parameter', async () => {
        const mem = newDb();
        const camelCasePlugin = new (await import('kysely')).CamelCasePlugin();
        const kysely = mem.adapters.createKysely(
            undefined,
            {
                plugins: [camelCasePlugin]
            }
        ) as Kysely<any>;
        const executor = kysely.getExecutor();
        expect(executor.plugins).toEqual([camelCasePlugin]);
    });

    it('should ignore dialect prop in kysely config', async () => {
        const mem = newDb();
        const kysely = mem.adapters.createKysely(
            undefined,
            {
                dialect: new (await import('kysely')).MysqlDialect({
                    pool: {} as any,
                })
            }
        ) as Kysely<any>;
        const executor = kysely.getExecutor();
        const ctorName = Object.getPrototypeOf(executor.adapter).constructor.name;
        expect(ctorName).toBe('PostgresAdapter');
        // expect(executor.adapter).toBeInstanceOf((await import('kysely')).PostgresAdapter);
    });
});
