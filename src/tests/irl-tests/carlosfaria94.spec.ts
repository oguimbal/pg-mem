import { newDb } from '../..';
import { expect } from 'chai';
import type { Knex } from 'knex';

// Objection has vulnerabilities: https://github.com/advisories/GHSA-r659-8xfp-j327
// import { knexSnakeCaseMappers } from 'objection'; 👉  just copy pasted the required utils
const { knexSnakeCaseMappers } = require('./objection-knexsnakecase.js');


async function up(knex: Knex): Promise<void> {
    return knex.schema
        .createTable('update_times', function (table) {
            table.string('id', 50).unique().notNullable().primary()
            table.dateTime('update_time').notNullable()
        })
        .createTable('room_board_types', function (table) {
            table.string('id', 2).unique().notNullable().primary()
            table.string('name', 100).notNullable()
            table.boolean('enabled').notNullable().defaultTo(false)
            table.boolean('package').notNullable().defaultTo(false)
        })
        .createTable('room_types', function (table) {
            table.string('id', 3).unique().notNullable().primary()
            table.string('name', 100).notNullable()
            table.boolean('enabled').notNullable().defaultTo(false)
            table.boolean('package').notNullable().defaultTo(false)
        })
        .createTable('room_characteristics', function (table) {
            table.string('id', 10).unique().notNullable().primary()
            table.string('name', 100).nullable()
            table.boolean('is_enabled').notNullable().defaultTo(true)
        })
        .createTable('room_types_characteristics', function (table) {
            table.string('type_id', 3).notNullable().references('id').inTable('room_types')
            table.string('characteristic_id', 10).notNullable().references('id').inTable('room_characteristics')
            table.primary(['type_id', 'characteristic_id'])
        })
}


describe('IRL tests', () => {
    it('carlosfaria94 x Knex migration fails', async () => {
        const mem = newDb();
        const knex = mem.adapters.createKnex(0, {
            // https://vincit.github.io/objection.js/api/objection/#knexsnakecasemappers
            ...knexSnakeCaseMappers(),
        }) as Knex;
        await up(knex);

        mem.public.none(`insert into room_characteristics(id,name,is_enabled) values ('roomid', 'roomname', true)`);


        const result = await knex('room_characteristics')
            .select('id', 'is_enabled');

        expect(result).to.deep.equal([{
            id: 'roomid',
            // snake cased
            isEnabled: true,
        }]);

    });
});
