import { newDb } from '../..'
import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, Connection } from 'typeorm'

@Entity({ name: 'account' })
export class Account extends BaseEntity {
    @PrimaryGeneratedColumn()
    id!: number

    @Column({ name: 'picture_url', type: 'text' })
    pictureUrl!: string
}



describe('IRL tests', () => {
    // https://github.com/rmanguinho/pg-mem-test

    let connection: Connection

    beforeEach(async () => {
        const db = newDb()
        connection = await db.adapters.createTypeormConnection({
            type: 'postgres',
            entities: [Account]
        })
        await connection.synchronize()
        /* await connection.query(`
          create table account (
            id serial primary key,
            picture_url varchar(200) null
          );
        `) */
    })

    beforeEach(async () => {
        await Account.clear()
    })

    afterEach(async () => {
        await connection.close()
    })

    it('rmanguinho x typeorm 0.2.30', async () => {
        const user = new Account()
        user.pictureUrl = 'any_url'
        await user.save()
        const updatedUser = await Account.findOne({ id: user.id })
        // expect(updatedUser.pictureUrl).toBe('any_url')
    })

})
