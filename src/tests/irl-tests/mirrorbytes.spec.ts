import { expect } from 'chai';
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
import { typeOrm } from '../test-utils';
import { DataType } from '../../interfaces';
import { v4 } from 'uuid';
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
    typeOrm('mirrobytes x typeorm', () => [User, Form, Submission], ({ mem }) => {
        mem.public.registerFunction({
            name: 'uuid_generate_v4',
            returns: DataType.uuid,
            implementation: v4,
        });
    }, async () => {

        const user = await User.create({
            name: 'me',
            email: 'me@me.com',
        }).save();
        const form = await Form.create({
            user,
        }).save();

        await Submission.create({
            form,
        }).save();


        const loaded = await User.find({
            where: {
                email: 'me@me.com'
            },
            relations: ['forms']
        });

        expect(loaded.length).to.equal(1);
    });
});
