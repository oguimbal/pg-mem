import 'mocha';
import 'chai';
import { checkSelect, checkInvalid } from './spec-utils';

describe('PG syntax: Select statements', () => {

    checkSelect(['select 42', 'select(42)'], {
        type: 'select',
        columns: [{
            type: 'integer',
            value: 42
        }],
    });

    checkSelect(['select 42, 53', 'select 42,53', 'select(42),53'], {
        type: 'select',
        columns: [{
            type: 'integer',
            value: 42
        }, {
            type: 'integer',
            value: 53
        }],
    });

    checkSelect(['select * from test', 'select*from"test"', 'select* from"test"', 'select *from"test"', 'select*from "test"', 'select * from "test"'], {
        type: 'select',
        from: [{ type: 'table', table: 'test' }],
        columns: [{ type: 'star' }]
    });

    checkSelect(['select * from db.test'], {
        type: 'select',
        from: [{ type: 'table', table: 'test', db: 'db' }],
        columns: [{ type: 'star' }]
    });

    checkSelect(['select a.*, b.*'], {
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

    checkSelect(['select a, b'], {
        type: 'select',
        columns: [
            { type: 'ref', name: 'a' },
            { type: 'ref', name: 'b' }]
    });


    checkSelect(['select * from test a where a.b > 42' // yea yea, all those are valid & equivalent..
        , 'select*from test"a"where a.b > 42'
        , 'select*from test as"a"where a.b > 42'
        , 'select*from test as a where a.b > 42'], {
        type: 'select',
        from: [{ type: 'table', table: 'test', alias: 'a' }],
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


    checkInvalid('select * from (select id from test)'); // <== missing alias

    checkSelect('select * from (select id from test) d', {
        type: 'select',
        columns: [{ type: 'star' }],
        from: [{
            type: 'statement',
            statement: {
                type: 'select',
                from: [{ type: 'table', table: 'test' }],
                columns: [{ type: 'ref', name: 'id' }],
            },
            alias: 'd'
        }]
    })
});
