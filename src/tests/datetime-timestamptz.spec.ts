import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('date/time functions on timestamptz', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
    });

    it('date_trunc on timestamptz', () => {
        expect(one(`select to_char(date_trunc('month', timestamptz '2020-06-15 10:30'), 'YYYY-MM-DD') as d`).d)
            .toEqual('2020-06-01');
    });

    it('date_part on timestamptz', () => {
        expect(one(`select date_part('year', timestamptz '2020-06-15') as y`).y).toEqual(2020);
    });

    it('to_timestamp from epoch', () => {
        expect(one(`select to_char(to_timestamp(1577836800), 'YYYY-MM-DD') as d`).d).toEqual('2020-01-01');
    });

    it('age between timestamptz values', () => {
        expect(one(`select age(timestamptz '2020-06-15', timestamptz '2020-01-01') as a`).a)
            .toEqual({ months: 5, days: 14 });
    });

    it('to_char on a date', () => {
        expect(one(`select to_char(date '2020-03-15', 'YYYY-MM-DD') as d`).d).toEqual('2020-03-15');
    });

    it('date_trunc on now() works end to end', () => {
        expect(one(`select date_trunc('day', now()) is not null as ok`).ok).toEqual(true);
    });
});
