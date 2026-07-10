import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('regexp functions', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('regexp_matches returns capture groups', () => {
        expect(one(`select regexp_matches('abc', '(a)(b)') as m`).m).toEqual(['a', 'b']);
    });

    it('regexp_matches returns the whole match when there are no groups', () => {
        expect(one(`select regexp_matches('foobar', 'o+') as m`).m).toEqual(['oo']);
    });

    it('regexp_matches with the g flag yields one row per match', () => {
        expect(many(`select regexp_matches('a1b2c3', '([a-z])([0-9])', 'g') as m`))
            .toEqual([{ m: ['a', '1'] }, { m: ['b', '2'] }, { m: ['c', '3'] }]);
    });

    it('regexp_split_to_array splits on a pattern', () => {
        expect(one(`select regexp_split_to_array('a1b2c', '[0-9]') as a`).a).toEqual(['a', 'b', 'c']);
    });

    it('regexp_split_to_table yields one row per piece', () => {
        expect(many(`select regexp_split_to_table('a,b,c', ',') as p`))
            .toEqual([{ p: 'a' }, { p: 'b' }, { p: 'c' }]);
    });

    it('honors the case-insensitive flag', () => {
        expect(one(`select regexp_matches('ABC', '(a)(b)', 'i') as m`).m).toEqual(['A', 'B']);
    });
});
