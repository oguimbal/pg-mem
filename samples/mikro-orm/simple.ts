import { Collection, Entity, ManyToOne, MikroORM, OneToMany, PrimaryKey, Property } from '@mikro-orm/core';

import { newDb } from '../../src/db';

@Entity()
export class Book {

    @PrimaryKey({ type: 'text' })
    id!: string;

    @Property({ type: 'text' })
    title!: string;

    @ManyToOne(() => Author)
    author!: Author;

}

@Entity()
export class Author {

    @PrimaryKey({ type: 'text' })
    id!: string;

    @Property({ type: 'text' })
    name!: string;

    @OneToMany(() => Book, book => book.author)
    books = new Collection<Book>(this);

    constructor(name: string) {
        this.name = name;
    }

}

export async function mikroOrmSample() {

    // create an instance of pg-mem
    const db = newDb();

    // bind an instance of mikro-orm to our pg-mem instance
    const orm: MikroORM = await db.adapters.createMikroOrm({
        entities: [Author, Book],
    });

    // create schema
    await orm.getSchemaGenerator().createSchema();

    // MikroORM started enforcing forking EM at some point
    // normally it's done in some kind of middleware etc.
    const forkedEm = orm.em.fork();

    // do things
    const books = forkedEm.getRepository(Book);
    const authors = forkedEm.getRepository(Author);

    const hugo = authors.create({
        id: 'hugo',
        name: 'Victor Hugo',
    });
    const miserables = books.create({
        id: 'miserables',
        author: hugo,
        title: 'Les Misérables',
    });

    await books.getEntityManager().persistAndFlush([hugo, miserables]);

    return db;
}
