import 'mocha';
import 'chai';
import { checkSelect, checkInvalid } from './spec-utils';
import { SelectedColumn, Expr, ExprBinary, JoinType, SelectStatement } from './ast';

describe('[PG syntax] Select statements', () => {


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



    function buildJoin(t: JoinType): SelectStatement {
        return {
            type: 'select',
            columns: noAlias([{ type: 'ref', name: '*' }]),
            from: [{
                type: 'table',
                table: 'ta'
            }, {
                type: 'table',
                table: 'tb',
                join: {
                    type: t,
                    on: {
                        type: 'binary',
                        op: '=',
                        left: {
                            type: 'ref',
                            table: 'ta',
                            name: 'id',
                        },
                        right: {
                            type: 'ref',
                            table: 'tb',
                            name: 'id',
                        },
                    }
                }
            }]
        }
    }

    checkInvalid('select * from ta full inner join tb on ta.id=tb.id');
    checkInvalid('select * from ta left inner join tb on ta.id=tb.id');
    checkInvalid('select * from ta right inner join tb on ta.id=tb.id');

    checkSelect(['select * from ta join tb on ta.id=tb.id'
        , 'select * from ta inner join tb on ta.id=tb.id']
        , buildJoin('INNER JOIN'));

    checkSelect(['select * from ta left join tb on ta.id=tb.id'
        , 'select * from ta left outer join tb on ta.id=tb.id']
        , buildJoin('LEFT JOIN'));

    checkSelect(['select * from ta right join tb on ta.id=tb.id'
        , 'select * from ta right outer join tb on ta.id=tb.id']
        , buildJoin('RIGHT JOIN'));


    checkSelect(['select * from ta full join tb on ta.id=tb.id'
        , 'select * from ta full outer join tb on ta.id=tb.id']
        , buildJoin('FULL JOIN'));


});
