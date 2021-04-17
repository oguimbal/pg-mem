import { newDb } from '../db';
import { mikroOrmSample } from '../../samples/mikro-orm/simple';
import { expect } from 'chai';

describe('Mikro ORM', () => {

    it('can perform sample', async () => {
        const db = await mikroOrmSample();

        expect(db.public.many(`select * from book`))
            .to.deep.equal([{
                id: 'miserables',
                author_id: 'hugo',
                title: 'Les Misérables',
            }]);

        expect(db.public.many(`select * from author`))
            .to.deep.equal([{
                id: 'hugo',
                name: 'Victor Hugo',
            }]);
    })
});
