import { sequelizeSample } from '../../samples/sequelize/sequelize';

describe('Sequelize', () => {

    it('can perform sample', async () => {
        await sequelizeSample();
    })
});
