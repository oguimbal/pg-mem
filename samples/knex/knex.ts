import { expect } from 'chai';
import { newDb } from '../../src/db';
import type { Knex } from "knex";

export async function knexSample() {

    // ========= CONNECT ==========

    // Create a new DB instance
    const mem = newDb();

    // create a Knex instance bound to this db
    //  =>  This replaces require('knex')({ ... })
    const knex = mem.adapters.createKnex() as Knex;


    // ========= USE AS USUAL ==========

    // Create a table
    await knex.schema
        .createTable('users', table => {
            table.increments('id');
            table.string('user_name');
        })
        // ...and another
        .createTable('accounts', table => {
            table.increments('id');
            table.string('account_name');
            table
                .integer('user_id')
                .unsigned()
                .references('users.id');
        })

    // Then query user table...
    await knex('users').insert({ user_name: 'Tim' });


    // ... and check
    expect(mem.public.many('select * from users'))
        .to.deep.equal([{
            id: 1,
            user_name: 'Tim',
        }]);

    // Then insert into account table...
    await knex('accounts').insert({ account_name: 'knex', user_id: 1 })


    // ... and check
    expect(mem.public.many('select * from accounts'))
        .to.deep.equal([{
            id: 1,
            account_name: 'knex',
            user_id: 1,
        }]);


    // Try to run a join
    const selectedRows = await knex('users')
        .join('accounts', 'users.id', 'accounts.user_id')
        .select('users.user_name as user', 'accounts.account_name as account')

    expect(selectedRows)
        .to.deep.equal([
            { user: 'Tim', account: 'knex' },
        ])


}
