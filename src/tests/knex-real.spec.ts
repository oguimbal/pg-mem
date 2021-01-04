import { knexSample } from '../../samples/knex/knex';

describe('Knex', () => {

    it('can perform sample', async () => {
        await knexSample();
    })
});