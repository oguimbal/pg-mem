import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('unnest WITH ORDINALITY', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
    });

    it('appends a 1-based ordinality column', () => {
        expect(many(`select * from unnest(array['a', 'b', 'c']) with ordinality`))
            .toEqual([
                { unnest: 'a', ordinality: '1' },
                { unnest: 'b', ordinality: '2' },
                { unnest: 'c', ordinality: '3' },
            ]);
    });

    it('honors alias column names', () => {
        expect(many(`select idx, val from unnest(array['x', 'y']) with ordinality as u(val, idx) order by idx desc`))
            .toEqual([{ idx: '2', val: 'y' }, { idx: '1', val: 'x' }]);
    });
});

describe('jsonb_pretty and #-', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
    });

    it('jsonb_pretty formats with 4-space indent', () => {
        expect(one(`select jsonb_pretty('{"a":1,"b":[1,2]}'::jsonb) as r`).r)
            .toEqual('{\n    "a": 1,\n    "b": [\n        1,\n        2\n    ]\n}');
    });

    it('#- removes an object key', () => {
        expect(one(`select '{"a":1,"b":2}'::jsonb #- '{a}' as r`).r).toEqual({ b: 2 });
    });

    it('#- removes an array element by index', () => {
        expect(one(`select '[1,2,3]'::jsonb #- '{1}' as r`).r).toEqual([1, 3]);
    });

    it('#- removes a nested path', () => {
        expect(one(`select '{"a":{"b":2,"c":3}}'::jsonb #- '{a,b}' as r`).r).toEqual({ a: { c: 3 } });
    });
});
