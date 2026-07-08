import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

// every expected value in this file has been verified against a real postgres 16
// (tools/conformance differential mode)

describe('to_char', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
    });

    const tc = (call: string) => one(`select ${call} as v`).v;

    describe('numbers', () => {
        it('right-aligns with a sign column', () => {
            expect(tc(`to_char(125, '999')`)).toBe(' 125');
            expect(tc(`to_char(-125, '999')`)).toBe('-125');
            expect(tc(`to_char(12, '999')`)).toBe('  12');
            expect(tc(`to_char(0, '999')`)).toBe('   0');
        });

        it('marks overflow with #, keeping separators and sign', () => {
            expect(tc(`to_char(1234, '999')`)).toBe(' ###');
            expect(tc(`to_char(-1234, '999')`)).toBe('-###');
            expect(tc(`to_char(123456, '9,999')`)).toBe(' #,###');
        });

        it('renders decimals, blanking a zero integer part', () => {
            expect(tc(`to_char(0.1, '0.9')`)).toBe(' 0.1');
            expect(tc(`to_char(0.1, '9.9')`)).toBe('  .1');
            expect(tc(`to_char(0.1, '9.99')`)).toBe('  .10');
            expect(tc(`to_char(12.345, '99.9')`)).toBe(' 12.3');
            expect(tc(`to_char(12.355, '99.9')`)).toBe(' 12.4');
            expect(tc(`to_char(0, '9.99')`)).toBe('  .00');
        });

        it('thousands separators only appear next to digits', () => {
            expect(tc(`to_char(1234.567, '9,999.99')`)).toBe(' 1,234.57');
            expect(tc(`to_char(1234, '9,999')`)).toBe(' 1,234');
            expect(tc(`to_char(12, '9,999')`)).toBe('    12');
            expect(tc(`to_char(1485, '9G999')`)).toBe(' 1,485');
            expect(tc(`to_char(7, '9G999')`)).toBe('     7');
        });

        it('zero slots force padding', () => {
            expect(tc(`to_char(12, '0999')`)).toBe(' 0012');
        });

        it('FM suppresses padding', () => {
            expect(tc(`to_char(125, 'FM999')`)).toBe('125');
            expect(tc(`to_char(12.30, 'FM99.99')`)).toBe('12.3');
            expect(tc(`to_char(12.30, 'FM99.00')`)).toBe('12.30');
            expect(tc(`to_char(-12.5, 'FM99.9')`)).toBe('-12.5');
            expect(tc(`to_char(0, 'FM9.99')`)).toBe('0.');
        });

        it('explicit signs', () => {
            expect(tc(`to_char(125, 'S999')`)).toBe('+125');
            expect(tc(`to_char(-125, 'S999')`)).toBe('-125');
            expect(tc(`to_char(125, '999MI')`)).toBe('125 ');
            expect(tc(`to_char(-125, '999MI')`)).toBe('125-');
        });

        it('V multiplies instead of rendering a decimal point', () => {
            expect(tc(`to_char(12, '99V99')`)).toBe(' 1200');
        });

        it('quoted literals', () => {
            expect(tc(`to_char(485, '"Good number:"999')`)).toBe('Good number: 485');
        });
    });

    describe('timestamps', () => {
        const ts = `timestamp '2020-02-15 14:30:45.123'`;

        it('date and time patterns', () => {
            expect(tc(`to_char(${ts}, 'YYYY-MM-DD')`)).toBe('2020-02-15');
            expect(tc(`to_char(${ts}, 'HH24:MI:SS')`)).toBe('14:30:45');
            expect(tc(`to_char(${ts}, 'HH12:MI AM')`)).toBe('02:30 PM');
            expect(tc(`to_char(${ts}, 'HH:MI am')`)).toBe('02:30 pm');
            expect(tc(`to_char(${ts}, 'MS US')`)).toBe('123 123000');
        });

        it('names follow the pattern casing, padded to 9 chars', () => {
            expect(tc(`to_char(${ts}, 'Month')`)).toBe('February ');
            expect(tc(`to_char(${ts}, 'MONTH')`)).toBe('FEBRUARY ');
            expect(tc(`to_char(${ts}, 'month')`)).toBe('february ');
            expect(tc(`to_char(${ts}, 'Mon DD, YYYY')`)).toBe('Feb 15, 2020');
            expect(tc(`to_char(${ts}, 'Day')`)).toBe('Saturday ');
            expect(tc(`to_char(${ts}, 'Dy DDD')`)).toBe('Sat 046');
        });

        it('FM applies to the next token only', () => {
            expect(tc(`to_char(${ts}, 'FMMonth FMDD, YYYY')`)).toBe('February 15, 2020');
            expect(tc(`to_char(timestamp '2020-01-05 09:05:03', 'FMHH12:MI:SS')`)).toBe('9:05:03');
        });

        it('week, quarter and century patterns', () => {
            expect(tc(`to_char(${ts}, 'YY CC Q D ID IW WW')`)).toBe('20 21 1 7 6 07 07');
            expect(tc(`to_char(timestamp '2020-12-31 23:59:59', 'IYYY IW ID')`)).toBe('2020 53 4');
        });

        it('year digit grouping and quoted literals', () => {
            expect(tc(`to_char(date '2001-01-01', 'Y,YYY')`)).toBe('2,001');
            expect(tc(`to_char(${ts}, '"Year:" YYYY')`)).toBe('Year: 2020');
        });
    });

    describe('intervals', () => {
        it('maps fields directly', () => {
            expect(tc(`to_char(interval '15h 2m 12s', 'HH24:MI:SS')`)).toBe('15:02:12');
        });
    });
});
