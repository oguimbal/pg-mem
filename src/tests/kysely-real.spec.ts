import { newDb } from '../db';
import { kyselySample } from '../../samples/kysely/kysely';
import { expect } from 'chai';
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
        expect(executor.plugins).to.deep.equal([camelCasePlugin]);
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
        expect(executor.adapter).to.be.instanceOf((await import('kysely')).PostgresAdapter);
    });
});
