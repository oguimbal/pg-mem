import { Entity, PrimaryGeneratedColumn, Column, Connection } from "typeorm";
import { newDb } from '../src/db';

// Declare an entity
@Entity()
export class User {

    @PrimaryGeneratedColumn({ type: 'int' })
    id: number;

    @Column({ type: 'text' })
    firstName: string;

    @Column({ type: 'text' })
    lastName: string;

    @Column({ type: 'int' })
    age: number;

}


(async () => {
    // Create a Typeorm connection
    const got: Connection = await newDb().adapters.createTypeormConnection({
        type: 'postgres',
        entities: [User]
    });

    // create tables
    await got.synchronize();

})();


// HMR (only here for dev purposes !)
declare var module;
module.hot?.accept();