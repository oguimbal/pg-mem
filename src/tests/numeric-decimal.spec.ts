import { describe, it, expect } from 'bun:test';
import { Decimal } from '../datatypes/numeric';

const D = Decimal.fromText;

describe('Decimal (BigInt-backed)', () => {
    it('parses and round-trips', () => {
        expect(D('1.005').toString()).toBe('1.005');
        expect(D('9007199254740993').toString()).toBe('9007199254740993');
        expect(D('-42.0').toString()).toBe('-42');
        expect(D('.5').toString()).toBe('0.5');
        expect(D('1.5e3').toString()).toBe('1500');
        expect(D('120e-2').toString()).toBe('1.2');
    });

    it('scale rounding is half away from zero', () => {
        expect(D('1.005').round(2).toString()).toBe('1.01');
        expect(D('2.5').round(0).toString()).toBe('3');
        expect(D('-2.5').round(0).toString()).toBe('-3');
        expect(D('1.004').round(2).toString()).toBe('1');
    });

    it('division keeps 20 fractional digits', () => {
        expect(D('1').div(D('3')).toString()).toBe('0.33333333333333333333');
        expect(D('10').div(D('4')).toString()).toBe('2.5');
    });

    it('arithmetic preserves precision', () => {
        expect(D('9007199254740992').add(D('1')).toString()).toBe('9007199254740993');
        expect(D('0.1').add(D('0.2')).toString()).toBe('0.3');
        expect(D('1.5').mul(D('1.5')).toString()).toBe('2.25');
        expect(D('100').sub(D('0.01')).toString()).toBe('99.99');
    });

    it('compares correctly', () => {
        expect(D('1.10').compare(D('1.1'))).toBe(0);
        expect(D('1.2').compare(D('1.19'))).toBe(1);
        expect(D('-5').compare(D('3'))).toBe(-1);
    });

    it('rejects division by zero', () => {
        expect(() => D('1').div(D('0'))).toThrow(/division by zero/);
    });

    it('computes remainder truncated toward zero', () => {
        expect(D('5.5').mod(D('2')).toString()).toBe('1.5');
        expect(D('10').mod(D('3')).toString()).toBe('1');
        expect(D('-5.5').mod(D('2')).toString()).toBe('-1.5');
        expect(() => D('1').mod(D('0'))).toThrow(/division by zero/);
    });
});
