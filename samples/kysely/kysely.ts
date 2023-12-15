import { expect } from 'chai';
import { newDb } from '../../src/db';

type GeneratedAlways<T> = import('kysely').GeneratedAlways<T>;

interface DB {
    users: {
        id: GeneratedAlways<number>;
        user_name: string | null;
    };
    accounts: {
        id: GeneratedAlways<number>;
        account_name: string | null;
        user_id: number | null;
    };
}

export async function kyselySample() {
    
    // ========= CONNECT ==========

    // Create a new DB instance
    const mem = newDb();

    // create a Kysely instance bound to this db
    //  =>  This replaces new require('kysely').Kysely({ ... })
    const kysely = mem.adapters.createKysely() as import('kysely').Kysely<DB>;


    // ========= USE AS USUAL ==========

    // Create a table
    await kysely.schema
        .createTable('users')
        .addColumn('id', 'serial', (cb) => cb.primaryKey())
        .addColumn('user_name', 'varchar(255)')
        .execute();

    // ...and another
    await kysely.schema
        .createTable('accounts')
        .addColumn('id', 'serial', (cb) => cb.primaryKey())
        .addColumn('account_name', 'varchar(255)')
        .addColumn('user_id', 'integer', (cb) => cb.references('users.id'))
        .addForeignKeyConstraint('fk_user_id', ['user_id'], 'users', ['id'])
        .execute();

    // Then query user table...       
    await kysely.insertInto('users').values({ user_name: 'Tim' }).execute();

    // ... and check
    expect(mem.public.many('select * from users'))
        .to.deep.equal([{
            id: 1,
            user_name: 'Tim',
        }]);

    // Then insert into account table...
    await kysely
        .insertInto('accounts')
        .values({ account_name: 'kysely', user_id: 1 })
        .execute();

    // ... and check
    expect(mem.public.many('select * from accounts'))
        .to.deep.equal([{
            id: 1,
            account_name: 'kysely',
            user_id: 1,
        }]);

    // Try to run a join
    const selectedRows = await kysely.selectFrom('users')
        .innerJoin('accounts', 'users.id', 'accounts.user_id')
        .select(['users.user_name as user', 'accounts.account_name as account'])
        .execute();

    expect(selectedRows)
        .to.deep.equal([
            { user: 'Tim', account: 'kysely' },
        ]);
}
