import { assert, expect } from 'chai';
import {
    BaseEntity,
    PrimaryGeneratedColumn,
    CreateDateColumn,
    BeforeInsert,
    BeforeUpdate,
    Entity,
    Column,
    ManyToOne,
    OneToMany,
} from 'typeorm';
import { typeOrm, TypeormSetup } from '../test-utils';
import { DataType } from '../../interfaces';
import { v4 } from 'uuid';
import { typeormJoinsSample } from '../../../samples/typeorm/joins';
export abstract class External extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    readonly id!: string;

    @CreateDateColumn({ type: 'timestamp' })
    createdAt!: Date;

    @CreateDateColumn({ type: 'timestamp' })
    updatedAt!: Date;

    @BeforeInsert()
    @BeforeUpdate()
    async validate(): Promise<void> {
        // nop
    }
}


@Entity()
export class User extends External {

    @Column('citext', { unique: true })
    email!: string;

    @Column({ type: 'text' })
    name!: string;

    @Column({ type: 'bytea' })
    password!: Buffer;

    @OneToMany(() => Form, (form) => form.user)
    forms!: Form[];

}

@Entity()
export class Form extends External {
    @Column({ type: 'text' })
    name!: string;

    @ManyToOne(() => User, (user) => user.forms)
    user!: User;


    @OneToMany(() => Submission, (submission) => submission.form, {
        cascade: true,
    })
    submissions!: Submission[];
}


@Entity()
export class Submission extends External {

    @ManyToOne(() => Form, (form) => form.submissions, { onDelete: 'CASCADE' })
    form!: Form;
}


describe('IRL tests', () => {

    const setup: TypeormSetup = ({ mem }) => {
        mem.registerExtension('uuid-ossp', (schema) => {
            schema.registerFunction({
                name: 'uuid_generate_v4',
                returns: DataType.uuid,
                implementation: v4,
            });
        });

        mem.registerExtension('citext', (schema) => {
            schema.registerFunction({
                name: 'citext',
                args: [DataType.text],
                returns: DataType.text,
                implementation: (arg: string) => arg.toLocaleLowerCase(),
            });
        });
    };

    typeOrm('mirrobytes x typeorm', () => [User, Form, Submission], setup, async () => {

        // test creations
        const user = await User.create({
            name: 'me',
            email: 'me@me.com',
            password: Buffer.from('pwd'),
        }).save();
        const form = await Form.create({
            name: 'form name',
            user,
        }).save();

        await Submission.create({
            form,
        }).save();


        // test user query result
        const loaded = await User.find({
            where: {
                email: 'me@me.com'
            },
            relations: ['forms']
        });

        expect(loaded.length).to.equal(1);
        const buf = loaded[0].password;
        assert.instanceOf(buf, Buffer);
        expect(buf.toString('utf-8')).to.equal('pwd');


        // test form query result
        const loaded_form = await Form.find({ user });
        assert.exists(loaded_form);
        expect(loaded.length).to.equal(1);
    });



    typeOrm('does not warns unhandled rejections', () => [User, Form, Submission], setup, async () => {
        let unhandled = false;
        const check = () => {
            unhandled = true;
        };
        process.on('unhandledRejection', check);
        try {
            let threw = false;
            try {
                await User.find({ id: '' });
            } catch (e) {
                // nop
                threw = true;
            }

            assert.isTrue(threw, 'Was expecting to throw...')

            // just wait a bit until the unhandled exception has been logged
            await new Promise(d => setTimeout(d, 1));

            // this used to throw :(
            assert.isFalse(unhandled, 'Unhandled exception raised !');

        } finally {
            process.off('unhandledRejection', check);
        }
    })
});
