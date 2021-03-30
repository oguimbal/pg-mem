import { newDb } from '../db';
import { knexSample } from '../../samples/knex/knex';
import { expect } from 'chai';

describe('Knex', () => {

    it('can perform sample', async () => {
        await knexSample();
    })


    it('bugfix on joins', async () => {
        const mem = newDb();
        const knex = mem.adapters.createKnex() as import('knex');


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
});