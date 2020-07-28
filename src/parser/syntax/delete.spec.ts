import 'mocha';
import 'chai';
import { checkDelete } from './spec-utils';

describe('[PG syntax] Delete', () => {

    checkDelete([`delete from test where a = b`], {
        type: 'delete',
        from: { table: 'test' },
        where: {
            type: 'binary',
            op: '=',
            left: { type: 'ref', name: 'a' },
            right: { type: 'ref', name: 'b' },
        }
    });


    checkDelete([`truncate test`, `truncate table test`], {
        type: 'delete',
        from: { table: 'test' },
    });

    checkDelete([`delete from test returning *`], {
        type: 'delete',
        from: { table: 'test' },
        returning: [{
            expr: { type: 'ref', name: '*' }
        }]
    });
});