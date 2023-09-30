import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces-private';
import { expectSingle } from './test-utils';

describe('Conversions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let one: (str: string) => any;
    function all(table = 'test') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    it('varchar(n) with insert too long', () => {
        db.public.none(`create table test(value varchar(5))`);
        assert.throws(() => {
            db.public.none(`insert into test(value) values ('12345678')`);
        });
    });

    it('compatible decimal with string', () => {
        db.public.none(`create table test(value decimal)`);
        db.public.none(`insert into test(value) values ('42.5')`);
        const many = db.public.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42.5 }]);
    });

    it('incompatible decimal with string', () => {
        db.public.none(`create table test(value decimal)`);
        assert.throws(() => db.public.none(`insert into test(value) values ('blah')`));
    });

    it('compatible int with string', () => {
        db.public.none(`create table test(value int)`);
        db.public.none(`insert into test(value) values ('42')`);
        const many = db.public.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42 }]);
    });

    it('incompatible int with string', () => {
        db.public.none(`create table test(value int)`);
        assert.throws(() => db.public.none(`insert into test(value) values ('42.5')`));
    });


    describe('Implicit casts', () => {
        it('implicitely casts in case', () => {
            expect(many(`select  case when 2 > 1 then to_date('20170103','YYYYMMDD') else '2017-01-03' end as x;`))
                .to.deep.equal([{ x: new Date('2017-01-03') }]);
            expect(many(`select  case when 2 > 1 then to_date('20170103','YYYYMMDD') when 2 > 3 then '2017-01-03' end as x;`))
                .to.deep.equal([{ x: new Date('2017-01-03') }]);
            expect(many(`select  case when 2 > 1 then '2017-01-03' else to_date('20170103','YYYYMMDD') end as x;`))
                .to.deep.equal([{ x: new Date('2017-01-03') }]);
        });

        it('implicitely casts in +', () => {
            expect(many(`select  1.5 + 1 as x;`))
                .to.deep.equal([{ x: 2.5 }]);
            expect(many(`select  1 + 1.5 as x;`))
                .to.deep.equal([{ x: 2.5 }]);
        });

        describe('implicitely casts t and f to booleans', () => {
            for (const t of ['t', 'tr', 'true', 'T', 'TR', 'TRu', 'TruE']) {
                it('casts "' + t + '" to true', () => {
                    expect(many(`select  '${t}'=true as x;`))
                        .to.deep.equal([{ x: true }]);
                })
            }
            for (const f of ['f', 'fa', 'false', 'F', 'Fa', 'Fal', 'FALSE']) {
                it('casts "' + f + '" to false', () => {
                    expect(many(`select  '${f}'=false as x;`))
                        .to.deep.equal([{ x: true }]);
                })
            }

            it('casts "t" to true in case', () => {
                expect(many(`select case true when 't' then 'yes' else 'no' end as x`))
                    .to.deep.equal([{ x: 'yes' }]);
            })
        });

        it('implicitely casts in + from int table', () => {
            none('create table test(num int); insert into test values (1)')
            expect(many(`select  1.5 + num as x from test`))
                .to.deep.equal([{ x: 2.5 }]);
            expect(many(`select  num + 1.5 as x from test`))
                .to.deep.equal([{ x: 2.5 }]);
        });

        it('implicitely casts in + from float table', () => {
            none('create table test(num float); insert into test values (1.5)')
            expect(many(`select  1 + num as x from test`))
                .to.deep.equal([{ x: 2.5 }]);
            expect(many(`select  num + 1 as x from test`))
                .to.deep.equal([{ x: 2.5 }]);
        });

        it('implicitely casts int & string', () => {
            expect(many(`select 1 = '1' as x;`))
                .to.deep.equal([{ x: true }]);
        })

        it('implicitely casts float & string', () => {
            expect(many(`select 1.1 = '1.10' as x;`))
                .to.deep.equal([{ x: true }]);
        });

        it('does not implicitely cast float & string int', () => {
            assert.throws(() => many(`select 1 = '1.10' as x;`));
        });


        it('does not implicitely casts on operations even constant on case', () => {
            assert.throw(() => many(`select  case when 2 > 1 then to_date('20170103','YYYYMMDD') else ('2017-' || '01-03') end as x;`));
        });

        it('does not implicitely casts on operations even constant on comparison', () => {
            expect(many(`select to_date('20170103','YYYYMMDD') > '2017-01-03' as x;`))
                .to.deep.equal([{ x: false }]);
            assert.throw(() => many(`select to_date('20170103','YYYYMMDD') > ('2017-' || '01-03') as x;`));
        })

    })


    it('parses an interval literal', () => {
        expect(many(`SELECT INTERVAL 'P2M' as v`))
            .to.deep.equal([{ v: { months: 2 } }]);
    });

    describe('Time', () => {
        expectSingle(`select time '23:18'`, '23:18:00');
        expectSingle(`select time '23:18.5'`, '00:23:18.5');
        expectSingle(`select time '23:18.005'`, '00:23:18.005');
        expectSingle(`select time '1:2:3'`, '01:02:03');
    })

    it('can cast integer to text', () => {
        expect(many(`SELECT 42::text`))
            .to.deep.equal([{ text: '42' }]);
    })

    it('can cast bool', () => {
        expect(one('select true::bool as val')).to.deep.equal({ val: true });
        expect(one('select true::int as val')).to.deep.equal({ val: 1 });
        expect(one('select true::text as val')).to.deep.equal({ val: 'true' });
    });

    it('can cast float to text', () => {
        expect(many(`SELECT 42.3::text`))
            .to.deep.equal([{ text: '42.3' }]);
    })

    it('can cast jsonb to float', () => {
        expect(many(`select '42.3'::jsonb::float val`))
            .to.deep.equal([{ val: 42.3 }]);
        expect(many(`select null::jsonb::float val`))
            .to.deep.equal([{ val: null }]);
    });

    it('can cast jsonb to int', () => {
        expect(many(`select '42.3'::jsonb::int val`))
            .to.deep.equal([{ val: 42 }]);
        expect(many(`select '42.5'::jsonb::int val`))
            .to.deep.equal([{ val: 43 }]);
        expect(many(`select null::jsonb::int val`))
            .to.deep.equal([{ val: null }]);
    });

    it('can cast jsonb to bool', () => {
        expect(many(`select 'true'::jsonb::boolean val`))
            .to.deep.equal([{ val: true }]);
    });

    it('can cast empty array', () => {
        expect(many(`select array[]::text[] val`))
            .to.deep.equal([{ val: [] }]);
    })

    it('can cast to timestamp with explicit precision', () => {
        expect(many(`select '2021-09-18 00:00:00Z'::timestamp(4) with time zone val`))
            .to.deep.equal([{ val: new Date('2021-09-18 00:00:00Z') }]);
    });


    it('implicitely converts 0 & 1 to boolean', () => {
        expect(many(`create table test(id text, value boolean);
            insert into test(id, value) values ('zero', '0');
            insert into test(id, value) values ('one', '1');
            select * from test`))
            .to.deep.equal([{ id: 'zero', value: false }, { id: 'one', value: true }]);

        expect(many(`select id from test where value = '1'`)).to.deep.equal([{ id: 'one' }]);
        expect(many(`select id from test where value = '0'`)).to.deep.equal([{ id: 'zero' }]);
    });

    it('should convert to text', () => {
        // to fix (should be text)
        expect(many(`select '{"b":51, "a":42}'::jsonb::text col`))
            .to.deep.equal([{ col: '{"a":42,"b":51}' }]);
        expect(many(`select '"abc"'::jsonb::text col`))
            .to.deep.equal([{ col: '"abc"' }]);
    });


    function usersAndData() {
        none(`create table user_data(usr int, data text);
        insert into user_data(usr, data) values (1, 'a'), (1, 'b'), (1, 'b'), (2, 'c');
        create table users(id int, value jsonb);
        insert into users(id) values (1);`);
    }
    it('casts aggregation selection to single value', () => {
        usersAndData();
        // used to fail
        expect(many(`
            UPDATE users
            SET
                value = (select jsonb_agg(distinct data) from user_data WHERE usr=1)
            WHERE id = 1;
            select * from users;
            `))
            .to.deep.equal([{ id: 1, value: ['a', 'b'] }])

    })

    it('cannot cast multiple results to single value', () => {
        usersAndData();
        // used to fail
        assert.throws(() => many(`
            UPDATE users
            SET
                value = (select jsonb_agg(distinct data), 42 bla from user_data WHERE usr=1)
            WHERE id = 1;
            select * from users;
            `), /subquery must return only one column/)

    })
});
