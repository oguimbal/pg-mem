import 'mocha';
import 'chai';
import { checkCreateIndex, checkInvalid } from './spec-utils';

describe('PG syntax: Create index', () => {

    checkCreateIndex(['create index blah on test(col)'], {
        type: 'create index',
        indexName: 'blah',
        table: 'test',
        expressions: [{
            expression: { type: 'ref', name: 'col' },
        }],
    });
    checkCreateIndex(['create index on test(col)'], {
        type: 'create index',
        table: 'test',
        expressions: [{
            expression: { type: 'ref', name: 'col' },
        }],
    });

    checkInvalid(`create index on test('a')`);
    checkInvalid('create index on test(a * 2)');
    checkInvalid('create index on test(a and b)');

    checkCreateIndex(['create index on test((a * 2))'], {
        type: 'create index',
        table: 'test',
        expressions: [{
            expression: {
                type: 'binary',
                op: '*',
                left: { type: 'ref', name: 'a' },
                right: { type: 'integer', value: 2 },
            }
        }],
    });

    checkCreateIndex(['CREATE INDEX ON test((a and 2))'], {
        type: 'create index',
        table: 'test',
        expressions: [{
            expression: {
                type: 'binary',
                op: 'AND',
                left: { type: 'ref', name: 'a' },
                right: { type: 'integer', value: 2 },
            }
        }],
    });

    checkCreateIndex(['create index on test(LOWER(a))', 'create index on test( ( lower(a) ) )'], {
        type: 'create index',
        table: 'test',
        expressions: [{
            expression: {
                type: 'call',
                function: 'lower',
                args: [{ type: 'ref', name: 'a' }]
            }
        }],
    });

    checkCreateIndex(['create unique index if not exists "abc" on test(LOWER(a) DESC NULLS LAST)'], {
        type: 'create index',
        table: 'test',
        ifNotExists: true,
        unique: true,
        indexName: 'abc',
        expressions: [{
            expression: {
                type: 'call',
                function: 'lower',
                args: [{ type: 'ref', name: 'a' }]
            },
            nulls: 'last',
            order: 'desc',
        }],
    });
});