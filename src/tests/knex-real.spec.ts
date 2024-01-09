import { newDb } from '../db';
import { knexSample } from '../../samples/knex/knex';
import { expect } from 'chai';
import type { Knex } from 'knex'

describe('Knex', () => {

    it('can perform sample', async () => {
        await knexSample();
    })


    it('bugfix on joins', async () => {
        const mem = newDb();
        const knex = mem.adapters.createKnex() as Knex;


        await knex.schema
            .createTable('TableGroups', table => {
                table.string('id');
                table.string('name');
            })
            .createTable('TableUsersGroup', table => {
                table.string('group_id');
                table.string('user_id');
            });


        await knex('TableGroups').insert({ id: 'gid', name: 'gname' });
        await knex('TableUsersGroup').insert({ user_id: 'uid', group_id: 'gid' });

        const result = await knex('TableGroups')
            .innerJoin('TableUsersGroup', 'group_id', 'id')
            .where('user_id', 'uid');

        expect(result)
            .to.deep.equal([
                { id: 'gid', name: 'gname', user_id: 'uid', group_id: 'gid' }
            ]);
    })

    it('should use knex config from parameter', async () => {
        const mem = newDb();
        const knex = mem.adapters.createKnex(
            undefined,
            {
                migrations: {
                    tableName: 'example_table',
                    directory: '.example_migrations',
                },
            }
        ) as Knex;
        const migrateConfig = knex.migrate;
        // TODO check knex 2.5.1 for migration config params stored
        //expect(migrateConfig.tableName).to.equal('example_table');
        //expect(migrateConfig.directory).to.equal('.example_migrations');
    })

    it('can name a column "group"', async () => {
        // https://github.com/oguimbal/pg-mem/issues/142
        const mem = newDb();
        const knex = mem.adapters.createKnex() as Knex;
        await knex.schema.createTable('table1', (table: Knex.TableBuilder) => {
            table.text('group').primary();
        });
        mem.public.many('select * from table1');
    })
});
