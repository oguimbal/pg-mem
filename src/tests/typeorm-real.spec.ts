import 'mocha';
import 'chai';
import { expect, assert } from 'chai';
import { typeormSimpleSample } from '../../samples/typeorm/simple';
import { Photo, typeormJoinsSample, User } from '../../samples/typeorm/joins';
import { _IDb } from '../interfaces-private';
import { Entity, BaseEntity, PrimaryColumn, PrimaryGeneratedColumn, Column, Connection } from 'typeorm';
import { typeOrm } from './test-utils';

describe('Typeorm - real manips', () => {
    typeOrm('handles jsonb update', () => [WithJsonb], null, async ({ db }) => {
        const repo = db.getRepository(WithJsonb);
        const got = repo.create({
            data: [{ someData: true }]
        });
        await got.save();
        let all = await repo.findByIds([1]);
        expect(all.map(x => x.data)).to.deep.equal([[{ someData: true }]]);
        got.data = { other: true };
        await got.save();
        all = await repo.find();
        expect(all.map(x => x.data)).to.deep.equal([{ other: true }]);
    });

    it('can perform simple sample', async () => {
        await typeormSimpleSample();
    })

    it('can perform join sample', async () => {
        await typeormJoinsSample();
    });

    typeOrm('can query relations', () => [Photo, User], null, async ({ db }) => {
        const photos = db.getRepository(Photo);

        await photos.find({
            where: { id: 42 },
            relations: ['user']
        });
    });
});

@Entity()
class WithJsonb extends BaseEntity {
    @PrimaryGeneratedColumn({ type: 'integer' })
    id!: number;

    @Column({ type: 'jsonb' })
    data: any;
}