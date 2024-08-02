import { describe, it, beforeEach, expect } from 'bun:test';
import { watchUse, queryJson } from '../utils';

describe('Test utils', () => {
    it('checkUse() checks everything', () => {
        const old = globalThis.process.env['NOCHECKFULLQUERYUSAGE'];
        delete globalThis.process.env['NOCHECKFULLQUERYUSAGE'];
        try {
            {
                const { checked, check } = watchUse({ a: 1, b: [{ c: 1 }] });
                checked.a;
                checked.b[0].c;
                expect(check!()).toBeFalsy();
            }
            {
                const { checked, check } = watchUse({ a: 1, b: [{ c: 1 }] });
                checked.a;
                expect(check!()).toBeTruthy();
            }
            {
                const { checked, check } = watchUse({ a: 1, b: [{ c: 1 }, { d: 1 }] });
                checked.a;
                checked.b[1].c;
                expect(check!()).toBeTruthy();
            }
            {
                const { checked, check } = watchUse({ a: 1, b: [{ c: 1 }, 5] });
                checked.a;
                (checked.b[0] as any).c;
                expect(check!()).toBeFalsy();
            }
        } finally {
            globalThis.process.env['NOCHECKFULLQUERYUSAGE'] = old;
        }
    });


    it('queryJson() works', () => {
        expect(queryJson({ a: 1 }, { a: 1, b: 2 })).toBeTrue();
        expect(queryJson([{ a: 1 }], { a: 1, b: 2 })).toBeFalse();
        expect(queryJson([{ a: 1 }], [{ a: 1, b: 2 }])).toBeTrue();
        expect(queryJson({ a: 1 }, [{ a: 1, b: 2 }])).toBeFalse();
        expect(queryJson({ a: [1] }, { a: [1, 2, 3] })).toBeTrue();
        expect(queryJson({ a: [{ b: 'test' }] }, { a: [{ b: 'test', c: 42 }] })).toBeTrue();
        expect(queryJson({ a: [{ b: 'test' }, { c: 12 }] }, { a: [{ c: 12 }, { b: 'test' }] })).toBeTrue();
    });
});
