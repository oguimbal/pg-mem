import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('quote_* / generate_subscripts / make_timestamp / make_time', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('quote_ident quotes when needed', () => {
        expect(one(`select quote_ident('foo bar') as v`).v).toEqual('"foo bar"');
        expect(one(`select quote_ident('foo') as v`).v).toEqual('foo');
    });

    it('quote_literal / quote_nullable', () => {
        expect(one(`select quote_literal('a''b') as v`).v).toEqual(`'a''b'`);
        expect(one(`select quote_nullable(null) as v`).v).toEqual('NULL');
    });

    it('generate_subscripts yields array indices', () => {
        expect(many(`select generate_subscripts(array[10, 20, 30], 1) as v`))
            .toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);
    });

    it('make_timestamp / make_time', () => {
        expect(one(`select to_char(make_timestamp(2020, 6, 15, 10, 30, 0), 'YYYY-MM-DD HH24:MI') as v`).v)
            .toEqual('2020-06-15 10:30');
        expect(one(`select make_time(10, 30, 0) as v`).v).toEqual('10:30:00');
    });
});
