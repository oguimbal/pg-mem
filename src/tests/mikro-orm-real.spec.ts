import { newDb } from '../db';
import { mikroOrmSample } from '../../samples/mikro-orm/simple';
import { describe, it, beforeEach, expect } from 'bun:test';

describe('Mikro ORM', () => {

    it('can perform sample', async () => {
        const db = await mikroOrmSample();

        expect(db.public.many(`select * from book`))
            .toEqual([{
                id: 'miserables',
                author_id: 'hugo',
                title: 'Les Misérables',
            }]);

        expect(db.public.many(`select * from author`))
            .toEqual([{
                id: 'hugo',
                name: 'Victor Hugo',
            }]);
    })
});
