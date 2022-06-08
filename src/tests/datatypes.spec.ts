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
    });

    describe('implicit conversions between date & time types', () => {
        it('implicit casts to timestamptz', () => {
            many(`create function test_fn (a timestamptz) returns int as $$ select 42 $$ language sql;`);

            many(`select test_fn(now()::timestamp)`);
            many(`select test_fn(now()::timestamptz)`);
            many(`select test_fn(now()::date)`);
            assert.throws(() => many(`select test_fn(now()::time)`), /function test_fn\(time without time zone\) does not exist/);
            assert.throws(() => many(`select test_fn(now()::timetz)`), /function test_fn\(time with time zone\) does not exist/);
        });

        it('implicit casts to timestamp', () => {
            many(`create function test_fn (a timestamp) returns int as $$ select 42 $$ language sql;`);

            many(`select test_fn(now()::timestamp)`);
            assert.throws(() => many(`select test_fn(now()::timestamptz)`), /function test_fn\(timestamp with time zone\) does not exist/);
            many(`select test_fn(now()::date)`);
            assert.throws(() => many(`select test_fn(now()::time)`), /function test_fn\(time without time zone\) does not exist/);
            assert.throws(() => many(`select test_fn(now()::timetz)`), /function test_fn\(time with time zone\) does not exist/);
        });

        it('implicit casts to date', () => {
            many(`create function test_fn (a date) returns int as $$ select 42 $$ language sql;`);

            assert.throws(() => many(`select test_fn(now()::timestamp)`), /function test_fn\(timestamp without time zone\) does not exist/);
            assert.throws(() => many(`select test_fn(now()::timestamptz)`), /function test_fn\(timestamp with time zone\) does not exist/);
            many(`select test_fn(now()::date)`);
            assert.throws(() => many(`select test_fn(now()::time)`), /function test_fn\(time without time zone\) does not exist/);
            assert.throws(() => many(`select test_fn(now()::timetz)`), /function test_fn\(time with time zone\) does not exist/);
        });

        it('implicit casts to time', () => {
            many(`create function test_fn (a time) returns int as $$ select 42 $$ language sql;`);

            assert.throws(() => many(`select test_fn(now()::timestamp)`), /function test_fn\(timestamp without time zone\) does not exist/);
            assert.throws(() => many(`select test_fn(now()::timestamptz)`), /function test_fn\(timestamp with time zone\) does not exist/);
            assert.throws(() => many(`select test_fn(now()::date)`), /function test_fn\(date\) does not exist/);
            many(`select test_fn(now()::time)`);
            assert.throws(() => many(`select test_fn(now()::timetz)`), /function test_fn\(time with time zone\) does not exist/);
        });

        it('implicit casts to timetz', () => {
            many(`create function test_fn (a timetz) returns int as $$ select 42 $$ language sql;`);

            assert.throws(() => many(`select test_fn(now()::timestamp)`), /function test_fn\(timestamp without time zone\) does not exist/);
            assert.throws(() => many(`select test_fn(now()::timestamptz)`), /function test_fn\(timestamp with time zone\) does not exist/);
            assert.throws(() => many(`select test_fn(now()::date)`), /function test_fn\(date\) does not exist/);
            many(`select test_fn(now()::time)`);
            many(`select test_fn(now()::timetz)`);
        });
    });

    describe('explicit conversions between date & time types', () => {
        it('casts to timestamptz', () => {
            many('select now()::timestamptz');

            many(`select now()::timestamp::timestamptz`);
            many(`select now()::timestamptz::timestamptz`);
            many(`select now()::date::timestamptz`);
            assert.throws(() => many(`select now()::time::timestamptz`), /cannot cast type time without time zone to timestamp with time zone/);
            assert.throws(() => many(`select now()::timetz::timestamptz`), /cannot cast type time with time zone to timestamp with time zone/);
        });
        it('casts to timestamp', () => {
            many('select now()::timestamp');
            many(`select now()::timestamp::timestamp`);
            many(`select now()::timestamptz::timestamp`);
            many(`select now()::date::timestamp`);
            assert.throws(() => many(`select now()::time::timestamp`), /cannot cast type time without time zone to timestamp without time zone/);
            assert.throws(() => many(`select now()::timetz::timestamp`), /cannot cast type time with time zone to timestamp without time zone/);
        });
        it('casts to date', () => {
            many('select now()::date');

            many(`select now()::timestamp::date`);
            many(`select now()::timestamptz::date`);
            many(`select now()::date::date`);
            assert.throws(() => many(`select now()::time::date`), /cannot cast type time without time zone to date/);
            assert.throws(() => many(`select now()::timetz::date`), /cannot cast type time with time zone to date/);
        });
        it('casts to time', () => {
            many('select now()::time');

            many(`select now()::timestamp::time`);
            many(`select now()::timestamptz::time`);
            assert.throws(() => many(`select now()::date::time`), /cannot cast type date to time/);
            many(`select now()::time::time`);
            many(`select now()::timetz::time`);
        });

        it('casts to timetz', () => {
            many('select now()::timetz');

            assert.throws(() => many(`select now()::timestamp::timetz`), /cannot cast type timestamp without time zone to time with time zone/);
            many(`select now()::timestamptz::timetz`);
            assert.throws(() => many(`select now()::date::timetz`), /cannot cast type date to time with time zone/);
            many(`select now()::time::timetz`);
            many(`select now()::timetz::timetz`);
        });
    });


    it('cannot call time function with date', () => {
        assert.throws(() => many(`
        create function test_fn (a time) returns int as $$ select 42 $$ language sql;
        select test_fn(now())`), /function test_fn\(timestamp with time zone\) does not exist/);
    });

    it('cannot create fn with "timestamp with time zone" (double quoted) type', () => {
        expect(many(`create function success (a timestamp with time zone) returns int as $$ select 42 $$ language sql;
                select success(now())`)).to.deep.equal([{ success: 42 }]);
        assert.throws(() => many(`create function failing (a "timestamp with time zone") returns int as $$ select 42 $$ language sql;`), /type "timestamp with time zone" does not exist/);
    });

    it('cannot call timestamp function with now()', () => {
        many(`create function test_fn (a timestamp) returns int as $$ select 42 $$ language sql;`);

        assert.throws(() => many(`select test_fn(now())`), /function test_fn\(timestamp with time zone\) does not exist/);
    });

    it('can build time from string', () => {
        many(`select time '19:22:25'`);
        many(`select timetz '19:22:25'`);
    })
});
