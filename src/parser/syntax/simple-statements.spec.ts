import 'mocha';
import 'chai';
import { checkStatement } from './spec-utils';
import { StartTransactionStatement, CommitStatement, Statement } from './ast';

describe('PG syntax: Simple statements', () => {

    checkStatement(['start transaction'], {
        type: 'start transaction',
    });

    checkStatement(['commit'], {
        type: 'commit',
    });

    checkStatement(['rollback'], {
        type: 'rollback',
    });
});