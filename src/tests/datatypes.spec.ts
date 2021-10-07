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

    it('cannot create record tables', () => {
        assert.throws(() => many('create table test(a record)'), /column "a" has pseudo-type record/);
    })

    describe('time', () => {
        it('can be casted', () => {
            expect(many(`select '00:35:19.383683'::time without time zone`))
                .to.deep.equal([{
                    // mind the zeros... we dont support sub millisecond.
                    'time without time zone': '00:35:19.383'
                }])

        })
    })


    describe('time', () => {
        it("2019-01-02T06:00:00.000Z = 2019-01-02T06:00:00.000Z is true", () => {
            expect(
                many(`create table test(val timestamp with time zone NOT NULL);
                    insert into test values ('2019-01-02T06:00:00.000Z');
                    select extract(epoch FROM val) as epoch from test WHERE val = '2019-01-02T06:00:00.000Z';`)
            ).to.deep.equal([{ epoch: 1546408800 }]);
        });
        it("2019-01-02T05:00:00.000Z = 2019-01-02T06:00:00.000Z is false", () => {
            expect(
                many(`create table test(val timestamp with time zone NOT NULL);
                    insert into test values ('2019-01-02T05:00:00.000Z');
                    select extract(epoch FROM val) as epoch from test WHERE val = '2019-01-02T06:00:00.000Z';`)
            ).to.deep.equal([]);
        });
        it("2019-01-02T07:00:00.000Z = 2019-01-02T06:00:00.000Z is false", () => {
            expect(
                many(`create table test(val timestamp with time zone NOT NULL);
                    insert into test values ('2019-01-02T07:00:00.000Z');
                    select extract(epoch FROM val) as epoch from test WHERE val = '2019-01-02T06:00:00.000Z';`)
            ).to.deep.equal([]);
        });
    })

    describe('inet', () => {
        it('can be inserted', () => {
            const valids = ['1.1.1.1', '1.2.255.0/0', '1.2.3.4/32'];
            expect(many(`CREATE TABLE example ( ip INET);
                            insert into example values (${valids.map(x => `'${x}'`).join('),(')});
                            select * from example`))
                .to.deep.equal(valids.map(ip => ({ ip })))
        })

        for (const i of ['256.0.0.0', '  1.1.1.1', '   1.1.1.1', '1.1.1', '1.1.1.1.3', '1.1.1.1/33', '01.1.1.1', '1.1.1.01']) {
            it(`"${i}" is invalid`, () => {
                assert.throws(() => many(`CREATE TABLE example ( ip INET);
                                insert into example values ('${i}')`), /invalid input syntax for type inet:/);
            })
        }
    })


    describe('jsonb', () => {
        it('[bugfix] supports negative array indexation', () => {
            expect(many(`select data -> 'val' ->  -1 as value from (values ('{"val": [1,2]}'::jsonb)) v(data)`))
                .to.deep.equal([{ value: 2 }]);
        });

        it('[bugfix] cannot cast implicitely jsonb boolean to pg boolean', () => {
            many(`create table test(data jsonb);
            insert into test values ('{"val": true}'), ('{"val": false}'), ('{}');`);

            // this was throwing
            assert.throws(() => many(`select * from test where data -> 'val'`), /argument of WHERE must be type boolean, not type jsonb/);
            assert.throws(() => many(`select * from test where data -> 'val' OR 1=1`), /argument of OR must be type boolean, not type jsonb/);
        })
    })
});
