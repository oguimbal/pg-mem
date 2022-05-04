import { sequelizeSample } from '../../samples/sequelize/sequelize';

describe.skip('Sequelize', () => {

    it('can perform sample without force sync', async () => {
        await sequelizeSample();
    })

    it('can perform sample with force sync', async () => {
        await sequelizeSample(true);
    });
});
