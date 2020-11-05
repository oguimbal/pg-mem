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
                const data = watchUse({ a: 1, b: [{ c: 1 }] });
                data.a;
                data.b[0].c;
                data.check!();
            }
            assert.throws(() => {
                const data = watchUse({ a: 1, b: [{ c: 1 }] });
                data.a;
                data.check!();
            });
            assert.throws(() => {
                const data = watchUse({ a: 1, b: [{ c: 1 }, { d: 1 }] });
                data.a;
                data.b[1].c;
                data.check!();
            }); {

                const data = watchUse({ a: 1, b: [{ c: 1 }, 5] });
                data.a;
                (data.b[0] as any).c;
                data.check!();
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
