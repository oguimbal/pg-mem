import 'mocha';
import 'chai';
import { checkSelect, checkInvalid } from './spec-utils';
import { SelectedColumn, Expr } from './ast';

describe('PG syntax: Select statements', () => {


    function noAlias(x: Expr[]): SelectedColumn[] {
        return x.map(expr => ({ expr }));
    }

    checkSelect(['select 42', 'select(42)'], {
        type: 'select',
        columns: noAlias([{
            type: 'integer',
            value: 42
        }]),
    });

    checkSelect(['select 42, 53', 'select 42,53', 'select(42),53'], {
        type: 'select',
        columns: noAlias([{
            type: 'integer',
            value: 42
        }, {
            type: 'integer',
            value: 53
        }]),
    });

    checkSelect(['select * from test', 'select*from"test"', 'select* from"test"', 'select *from"test"', 'select*from "test"', 'select * from "test"'], {
        type: 'select',
        from: [{ type: 'table', table: 'test' }],
        columns: noAlias([{ type: 'ref', name: '*' }])
    });

    checkSelect(['select * from current_schema()', 'select * from current_schema ( )'], {
        type: 'select',
        from: [{ type: 'table', table: 'current_schema' }],
        columns: noAlias([{ type: 'ref', name: '*' }])
    });

    checkSelect(['select a as a1, b as b1 from test', 'select a a1,b b1 from test', 'select a a1 ,b b1 from test'], {
        type: 'select',
        from: [{ type: 'table', table: 'test' }],
        columns: [{
            expr: { type: 'ref', name: 'a' },
            alias: 'a1',
        }, {
            expr: { type: 'ref', name: 'b' },
            alias: 'b1',
        }],
    });

    checkSelect(['select * from db.test'], {
        type: 'select',
        from: [{ type: 'table', table: 'test', db: 'db' }],
        columns: noAlias([{ type: 'ref', name: '*' }]),
    });

    checkSelect(['select a.*, b.*'], {
        type: 'select',
        columns: noAlias([{
            type: 'ref',
            name: '*',
            table: 'a',
        }, {
            type: 'ref',
            name: '*',
            table: 'b',
        }])
    });

    checkSelect(['select a, b'], {
        type: 'select',
        columns: noAlias([
            { type: 'ref', name: 'a' },
            { type: 'ref', name: 'b' }])
    });


    checkSelect(['select * from test a where a.b > 42' // yea yea, all those are valid & equivalent..
        , 'select*from test"a"where a.b > 42'
        , 'select*from test as"a"where a.b > 42'
        , 'select*from test as a where a.b > 42'], {
        type: 'select',
        from: [{ type: 'table', table: 'test', alias: 'a' }],
        columns: noAlias([{ type: 'ref', name: '*' }]),
        where: {
            type: 'binary',
            op: '>',
            left: {
                type: 'ref',
                table: 'a',
                name: 'b',
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
        columns: noAlias([{ type: 'ref', name: '*' }]),
        from: [{
            type: 'statement',
            statement: {
                type: 'select',
                from: [{ type: 'table', table: 'test' }],
                columns: noAlias([{ type: 'ref', name: 'id' }]),
            },
            alias: 'd'
        }]
    })
});
