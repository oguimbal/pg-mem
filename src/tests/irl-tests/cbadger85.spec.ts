import { describe, it, beforeEach, expect } from 'bun:test';

import {
    BaseEntity,
    DriverException,
    EntitySchema,
    MikroORM,
} from '@mikro-orm/core';
import { newDb } from '../..';
import { errorMessage } from '../../utils';

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
            expect(e).not.toBeInstanceOf(DriverException);
            expect(errorMessage(e)).toMatch(/Book not found/);
            return;
        }
        expect('Should have thrown').toBe('');
    });
});
