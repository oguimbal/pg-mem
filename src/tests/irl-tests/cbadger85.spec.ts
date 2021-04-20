import 'mocha';
import { assert, expect } from 'chai';
import {
    BaseEntity,
    DriverException,
    EntitySchema,
    MikroORM,
} from '@mikro-orm/core';
import { newDb } from '../..';

export class Book extends BaseEntity<Book, 'id'> {
    id!: number;
    title!: string;
    author!: string;
}

export const bookSchema = new EntitySchema<Book, BaseEntity<Book, 'id'>>({
    class: Book,
    extends: 'BaseEntity',
    properties: {
        id: { type: Number, primary: true },
        title: { type: 'string' },
        author: { type: 'string' },
    },
});

describe('IRL tests', () => {


    let orm: MikroORM;

    beforeEach(async () => {
        const db = newDb();
        orm = await db.adapters.createMikroOrm({
            entities: [bookSchema],
            debug: true,
        });

        await orm.getSchemaGenerator().createSchema();
    });

    it('cbadger85 x mikro-orm', async () => {
        try {
            await orm.em.getRepository(bookSchema).findOneOrFail({ id: 1 });
        } catch (e) {
            expect(e).not.to.be.instanceOf(DriverException);
            expect(e.message).to.match(/Book not found/);
            return;
        }
        assert.fail('Should have thrown');
    });
});
