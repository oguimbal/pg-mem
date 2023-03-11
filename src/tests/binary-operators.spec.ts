import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces-private';

describe('Binary operators', () => {

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



    it('[bugfix] can substract date & ints', () => {
        // was throwing (cannot cast type integer to date)
        //  => https://github.com/oguimbal/pg-mem/issues/172
        assert.instanceOf(one(`select CURRENT_DATE - 7 as dt`).dt, Date);
        assert.instanceOf(one(`select CURRENT_DATE + 7 as dt`).dt, Date);
        assert.instanceOf(one(`select 7 + CURRENT_DATE as dt`).dt, Date);

        // only works with ints
        assert.throws(() => many(`select 7 - CURRENT_DATE`), /operator does not exist: integer - date/);
    })

    it.skip('should not be able to substract float to date', () => {
        // these should throw (#todo)
        assert.throws(() => many(`select CURRENT_DATE - 7.5`), /operator does not exist: date - float/);
        assert.throws(() => many(`select CURRENT_DATE - 7.5::float`), /operator does not exist: date - float/);
    })


    it('supports remove key on json', () => {
        // bugfix of https://github.com/oguimbal/pg-mem/issues/77
        expect(one(`select '["a", "b", "b", "c"]'::jsonb - 'b' as test`))
            .to.deep.equal({ test: ['a', 'c'] });
        expect(one(`select '{"a": "a", "b":"b"}'::jsonb - 'b' as test`))
            .to.deep.equal({ test: { 'a': 'a' } });
    });

    it('supports remove index', () => {
        expect(one(`select '["a", "b", "b", "c"]'::jsonb - 0 as test`))
            .to.deep.equal({ test: ['b', 'b', 'c'] });
        expect(one(`select '["a", "b", "b", "c"]'::jsonb - 3 as test`))
            .to.deep.equal({ test: ['a', 'b', 'b'] });
    });


    // https://www.postgresql.org/docs/15/functions-json.html

    it('supports Is the first JSON value contained in the second?', () => {
        expect(one(`select '{"b":2}'::jsonb <@ '{"a":1, "b":2}'::jsonb as test`))
            .to.deep.equal({ test: true });
        expect(one(`select '{"a":1, "b":2}'::jsonb <@ '{"a":1, "b":2}'::jsonb as test`))
            .to.deep.equal({ test: true });
        expect(one(`select '{"c":3}'::jsonb <@ '{"a":1, "b":2}'::jsonb as test`))
        .to.deep.equal({ test: false });
    });

    it('supports Does the text string exist as a top-level key or array element within the JSON value?', () => {
        expect(one(`select '{"a":1, "b":2}'::jsonb ? 'a' as test`))
            .to.deep.equal({ test: true });
        expect(one(`select '{"a":1, "b":2}'::jsonb ? 'b' as test`))
            .to.deep.equal({ test: true });
        expect(one(`select '{"a":1, "b":2}'::jsonb ? 'c' as test`))
            .to.deep.equal({ test: false });

        expect(one(`select '["a", "b", "c"]'::jsonb ? 'b' as test`))
            .to.deep.equal({ test: true });
    });

    it('supports Do any of the strings in the text array exist as top-level keys or array elements?', () => {
        expect(one(`select '{"a":1, "b":2, "c":3}'::jsonb ?| array['b', 'd'] as test`))
            .to.deep.equal({ test: true });
        expect(one(`select '{"a":1, "b":2, "c":3}'::jsonb ?| array['b', 'c'] as test`))
            .to.deep.equal({ test: true });
        expect(one(`select '{"a":1, "b":2, "c":3}'::jsonb ?| array['d', 'e'] as test`))
            .to.deep.equal({ test: false });
    });

    it('supports Do all of the strings in the text array exist as top-level keys or array elements?', () => {
        expect(one(`select '{"a":1, "b":2, "c":3}'::jsonb ?& array['a', 'b'] as test`))
            .to.deep.equal({ test: true });

        expect(one(`select '["a", "b", "c"]'::jsonb ?& array['a', 'b'] as test`))
            .to.deep.equal({ test: true });
    });

});
