import { expect } from 'chai';
import { newDb } from '../../src/db';
import { Sequelize, DataTypes } from 'sequelize';

export async function sequelizeSample(force?: boolean) {

    // ========= CONNECT ==========

    // Create a new DB instance
    const mem = newDb();

    const sequelize = new Sequelize({
        dialect: 'postgres',
        dialectModule: mem.adapters.createPg(),
    });

    // await seq.authenticate();
    const User = sequelize.define('User', {
        // Model attributes are defined here
        firstName: {
            type: DataTypes.STRING,
            allowNull: false
        },
        lastName: {
            type: DataTypes.STRING
            // allowNull defaults to true
        }
    }, {
        // Other model options go here
    });

    await sequelize.sync({ force });
}