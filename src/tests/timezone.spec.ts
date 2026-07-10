import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

// verified against postgres 16 (session timezone = UTC)
describe('Timezones', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
    });

    it('timestamp AT TIME ZONE zone yields the instant', () => {
        expect(one(`select timestamp '2020-01-01 12:00:00' at time zone 'UTC' as r`).r)
            .toEqual(new Date('2020-01-01T12:00:00.000Z'));
        // America/New_York is -05 in January -> 12:00 local = 17:00 UTC
        expect(one(`select timestamp '2020-01-01 12:00:00' at time zone 'America/New_York' as r`).r)
            .toEqual(new Date('2020-01-01T17:00:00.000Z'));
    });

    it('fixed offsets in AT TIME ZONE', () => {
        expect(one(`select timestamp '2020-01-01 12:00:00' at time zone '+05' as r`).r)
            .toEqual(new Date('2020-01-01T07:00:00.000Z'));
    });

    it('timestamptz AT TIME ZONE zone yields wall-clock', () => {
        expect(one(`select (timestamp '2020-01-01 12:00:00' at time zone 'UTC')::timestamptz at time zone 'America/New_York' as r`).r)
            .toEqual(new Date('2020-01-01T07:00:00.000Z'));
    });

    it('renders timestamptz as text with the session offset', () => {
        const r = many(`set timezone = 'UTC'; select ('2020-01-01 12:00:00+05'::timestamptz)::text as r`).pop();
        expect(r).toEqual({ r: '2020-01-01 07:00:00+00' });
    });

    it('renders plain timestamp as text without a zone', () => {
        expect(one(`select (timestamp '2020-01-01 12:00:00')::text as r`).r).toBe('2020-01-01 12:00:00');
        expect(one(`select (timestamp '2020-01-01 12:00:00.123')::text as r`).r).toBe('2020-01-01 12:00:00.123');
    });
});
