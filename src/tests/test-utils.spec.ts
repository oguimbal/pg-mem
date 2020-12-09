import 'mocha';
import 'chai';
import { expect, assert } from 'chai';
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
                assert.notExists(check!());
            }
            {
                const { checked, check } = watchUse({ a: 1, b: [{ c: 1 }] });
                checked.a;
                assert.exists(check!());
            }
            {
                const { checked, check } = watchUse({ a: 1, b: [{ c: 1 }, { d: 1 }] });
                checked.a;
                checked.b[1].c;
                assert.exists(check!());
            }
            {
                const { checked, check } = watchUse({ a: 1, b: [{ c: 1 }, 5] });
                checked.a;
                (checked.b[0] as any).c;
                assert.notExists(check!());
            }
        } finally {
            globalThis.process.env['NOCHECKFULLQUERYUSAGE'] = old;
        }
    });


    it('queryJson() works', () => {
        assert.isTrue(queryJson({ a: 1 }, { a: 1, b: 2 }));
        assert.isFalse(queryJson([{ a: 1 }], { a: 1, b: 2 }));
        assert.isTrue(queryJson([{ a: 1 }], [{ a: 1, b: 2 }]));
        assert.isFalse(queryJson({ a: 1 }, [{ a: 1, b: 2 }]));
        assert.isTrue(queryJson({ a: [1] }, { a: [1, 2, 3] }));
        assert.isTrue(queryJson({ a: [{ b: 'test' }] }, { a: [{ b: 'test', c: 42 }] }));
        assert.isTrue(queryJson({ a: [{ b: 'test' }, { c: 12 }] }, { a: [{ c: 12 }, { b: 'test' }] }));
    })
});
