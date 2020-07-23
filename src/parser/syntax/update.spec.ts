import 'mocha';
import 'chai';
import { checkUpdate } from './spec-utils';

describe('PG syntax: Update', () => {

    checkUpdate([`update test set a=1`, `UPDATE"test"SET"a"=1`], {
        type: 'update',
        table: { table: 'test' },
        sets: [{
            column: 'a',
            value: { type: 'integer', value: 1 }
        }]
    });

    checkUpdate([`update test set a=1, b=a where a>1`], {
        type: 'update',
        table: { table: 'test' },
        sets: [{
            column: 'a',
            value: { type: 'integer', value: 1 }
        }, {
            column: 'b',
            value: { type: 'ref', name: 'a' },
        }],
        where: {
            type: 'binary',
            op: '>',
            left: { type: 'ref', name: 'a' },
            right: { type: 'integer', value: 1 },
        }
    });
});