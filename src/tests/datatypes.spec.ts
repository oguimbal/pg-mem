import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';

describe('Data types', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
    });


    describe('uuid', () => {

        for (const v of ['A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'
            , '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'
            , '{a0eebc99-9c0b4ef8bb6d6bb9bd380a11}'
            , 'a0eebc999c0b4ef8bb6d6bb9bd380a11']) {
            it(`can implicitely cast ${v} to uuid`, () => {
                many(`create table test(id uuid primary key);
                        insert into test values ('${v}');
                    `);

            })
        }

        for (const v of ['A0EEBC99B-9C0B-4EF8-BB6D-6BB9BD380A11'
            , '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
            , 'a0eebc99-9c0b4ef8bb6d6bb9bd380a11}'
            , 'a0eebc999c0b4ef8bb6d6bb9bd380a11b'
            , 'x0eebc999c0b4ef8bb6d6bb9bd380a11']) {
            it(`cannot implicitely cast ${v} to uuid`, () => {
                many(`create table test(id uuid primary key);`)
                assert.throws(() => many(`insert into test values ('${v}');`));
            })
        }

        it('can convert uuid to string', () => {
            const ret = many(`create table test(id uuid primary key);
                        insert into test values ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11');
                        select concat(id,'test') as c from test`);
            expect(ret).to.deep.equal([{
                c: 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11test',
            }]);
        })

        it('can checks unicity on different literals but same value', () => {
            many(`create table test(id uuid primary key);
                            insert into test values ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11')`);
            assert.throws(() => many(`insert into test values ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11')`));
            assert.throws(() => many(`insert into test values ('{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}')`));
            assert.throws(() => many(`insert into test values ('{a0eebc99-9c0b4ef8bb6d6bb9bd380a11}')`));
            assert.throws(() => many(`insert into test values ('a0eebc999c0b4ef8bb6d6bb9bd380a11')`));
        })
    });



    describe('bytea', () => {
        it('compare buffers', () => {
            const res = many(`create table test(a bytea, b bytea);
                insert into test values ('abc', 'def');
                insert into test values ('abc', 'abc');
                select * from test where a=b`);
            expect(res).to.be.lengthOf(1);
            expect(res[0]).to.have.property('b');
            const buf = res[0].b;
            assert.instanceOf(buf, Buffer);
            expect(buf.toString('utf-8')).to.equal('abc');

        });
    })

    describe('time', () => {
        it('can be casted', () => {
            expect(many(`select '00:35:19.383683'::time without time zone`))
                .to.deep.equal([{
                    // mind the zeros... we dont support sub second.
                    'time without time zone': '00:35:19.000000'
                }])

        })
    })
});