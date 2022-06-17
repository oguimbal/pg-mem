import { Entity, PrimaryGeneratedColumn, Column, Connection, BaseEntity, LessThan, DeleteDateColumn, OneToMany, ManyToOne } from "typeorm";
import { newDb } from '../../src/db';
import chai, { expect } from 'chai';
import shallow from 'chai-shallow-deep-equal';
chai.use(shallow);

@Entity()
export class User extends BaseEntity {

    @PrimaryGeneratedColumn({ type: 'integer' })
    id!: number;

    @Column({ type: 'text' })
    name!: string;

    @OneToMany(type => Photo, photo => photo.user)
    photos!: Photo[];
}


@Entity()
export class Photo extends BaseEntity {

    @PrimaryGeneratedColumn({ type: 'integer' })
    id!: number;

    @Column({ type: 'text' })
    url!: string;

    @ManyToOne(type => User, user => user.photos)
    user!: User;
}

export async function typeormJoinsSample() {

    //==== create a memory db
    const db = newDb({
        // 👉 Recommended when using Typeorm .synchronize(), which creates foreign keys but not indices !
        autoCreateForeignKeyIndices: true,
    });

    //==== create a Typeorm connection
    const got: Connection = await db.adapters.createTypeormConnection({
        type: 'postgres',
        entities: [User, Photo]
    });

    try {

        //==== create tables
        await got.synchronize();
        const users = got.getRepository(User);
        const photos = got.getRepository(Photo);

        //==== create entities
        await users.create({
            name: 'me',
            photos: [
                await photos.create({ url: 'photo-of-me-1.jpg' }).save(),
                await photos.create({ url: 'photo-of-me-2.jpg' }).save(),
            ]
        }).save();
        await users.create({
            name: 'you',
            photos: [
                await photos.create({ url: 'photo-of-you-1.jpg' }).save(),
                await photos.create({ url: 'photo-of-you-2.jpg' }).save(),
            ]
        }).save();

        //==== query entities
        const user = await users.createQueryBuilder('user')
            .leftJoinAndSelect('user.photos', 'photo')
            .where('user.name = :name', { name: 'me' })
            .getOne();

        expect(user).to.shallowDeepEqual({
            id: 1,
            name: 'me',
            photos: [{
                id: 1,
                url: 'photo-of-me-1.jpg'
            }, {
                id: 2,
                url: "photo-of-me-2.jpg"
            }]
        })

    } finally {
        // do not forget to close the connection once done...
        // ... typeorm stores connections in a static object,
        // and does not like opening 'default connections.
        await got.close();
    }
}
