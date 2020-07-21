import 'mocha';
import 'chai';
import { checkStatement } from './spec-utils';

describe('PG syntax: Insert', () => {

    checkStatement([`insert into test(a, b) values (1, 'x')`, `INSERT INTO"test"(a,"b")VALUES(1,'x')`], {
        type: 'insert',
        into: { table: 'test' },
        columns: ['a', 'b'],
        values: [[{
            type: 'integer',
            value: 1,
        }, {
            type: 'string',
            value: 'x',
        }]],
    });

    checkStatement([`insert into test(a) values (1)`], {
        type: 'insert',
        into: { table: 'test' },
        columns: ['a'],
        values: [[{
            type: 'integer',
            value: 1,
        },]],
    });

    checkStatement([`insert into db . test(a, b) values (1, 'x')`, `INSERT INTO"db"."test"(a,"b")VALUES(1,'x')`], {
        type: 'insert',
        into: { table: 'test', db: 'db' },
        columns: ['a', 'b'],
        values: [[{
            type: 'integer',
            value: 1,
        }, {
            type: 'string',
            value: 'x',
        }]]
    });



    checkStatement([`insert into db . test(a, b) select a,b FROM x . test`], {
        type: 'insert',
        into: { table: 'test', db: 'db' },
        columns: ['a', 'b'],
        select: {
            type: 'select',
            from: [{
                type: 'table',
                table: 'test',
                db: 'x'
            }],
            columns: [{
                expr: {
                    type: 'ref',
                    name: 'a',
                }
            }, {
                expr: {
                    type: 'ref',
                    name: 'b',
                }
            }],
        }
    });

    checkStatement([`insert into "test" select * FROM test`, `insert into test(select * FROM test)`], {
        type: 'insert',
        into: { table: 'test' },
        select: {
            type: 'select',
            from: [{
                type: 'table',
                table: 'test'
            }],
            columns: [{
                expr: {
                    type: 'ref',
                    name: '*',
                }
            }],
        }
    });
});