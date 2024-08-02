import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../../db';

import { _IDb } from '../../interfaces-private';
import path from 'path';

describe('Migrate', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('can run migrations', async () => {
        await db.public.migrate({
            migrationsPath: path.resolve('./src/tests/migrate'),
        });

        expect(many(`select id, name from migrations`))
            .toEqual([
                { id: 1, name: 'initial' }
                , { id: 2, name: 'some-feature' }
                , { id: 3, name: 'test-cert' }
                , { id: 4, name: 'no-down' }
            ])
    })

});