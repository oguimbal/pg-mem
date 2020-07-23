import { Entity, PrimaryGeneratedColumn, Column, Connection, BaseEntity, LessThan } from "typeorm";
import { newDb } from '../src/db';

// Declare an entity
@Entity()
export class User extends BaseEntity {

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
    //==== create a Typeorm connection
    const got: Connection = await newDb().adapters.createTypeormConnection({
        type: 'postgres',
        entities: [User]
    });

    //==== create tables
    await got.synchronize();
    const users = got.getRepository(User);

    //==== create entities
    await users.create({
        firstName: 'john',
        lastName: 'doe',
        age: 18,
    }).save();
    await users.create({
        firstName: 'john',
        lastName: 'lennon',
        age: 99,
    }).save();
    const duck = await users.create({
        firstName: 'donald',
        lastName: 'duck',
        age: 12,
    }).save();

    //==== query entities
    const youngJohns = await users.find({
        firstName: 'john',
        age: LessThan(30)
    });

    console.log(youngJohns.map(x => x.lastName)); // outputs 'doe' !


    //==== modify entities
    duck.firstName = 'daisy';
    await duck.save();

})();


// HMR (only here for dev purposes !)
declare var module;
module.hot?.accept();