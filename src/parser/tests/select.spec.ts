import 'mocha';
import 'chai';
import { checkTree } from './spec-utils';

describe('PG syntax: Select statements', () => {

    checkTree(['select 42', 'select(42)'], {
        type: 'select',
        columns: [{
            type: 'integer',
            value: 42
        }],
    });

    checkTree(['select 42, 53', 'select 42,53', 'select(42),53'], {
        type: 'select',
        columns: [{
            type: 'integer',
            value: 42
        }, {
            type: 'integer',
            value: 53
        }],
    });

    checkTree(['select * from test', 'select*from"test"', 'select* from"test"', 'select *from"test"', 'select*from "test"', 'select * from "test"'], {
        type: 'select',
        from: { subject: 'test' },
        columns: [{ type: 'star' }]
    });

    checkTree(['select a.*, b.*'], {
        type: 'select',
        columns: [{
            type: 'member',
            operand: {
                type: 'ref',
                name: 'a'
            },
            member: '*',
        }, {
            type: 'member',
            operand: {
                type: 'ref',
                name: 'b'
            },
            member: '*',
        }]
    });

    checkTree(['select a, b'], {
        type: 'select',
        columns: [
            { type: 'ref', name: 'a' },
            { type: 'ref', name: 'b' }]
    });


    checkTree(['select * from test a where a.b > 42' // yea yea, all those are valid & equivalent..
            , 'select*from test"a"where a.b > 42'
            , 'select*from test as"a"where a.b > 42'
            , 'select*from test as a where a.b > 42'], {
        type: 'select',
        from: { subject: 'test', alias: 'a' },
        columns: [{ type: 'star' }],
        where: {
            type: 'binary',
            op: '>',
            left: {
                type: 'member',
                operand: { type: 'ref', name: 'a' },
                member: 'b',
            },
            right: {
                type: 'integer',
                value: 42,
            },
        }
    });
});
