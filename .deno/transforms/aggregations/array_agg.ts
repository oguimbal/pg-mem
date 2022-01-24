import { AggregationComputer, AggregationGroupComputer, IValue, nil, QueryError, _ISelection, _IType, _Transaction } from '../../interfaces-private.ts';
import { ExprCall } from 'https://deno.land/x/pgsql_ast_parser@9.2.2/mod.ts';
import { buildValue } from '../../expression-builder.ts';
import { Types } from '../../datatypes/index.ts';


class ArrayAggExpr implements AggregationComputer<any[]> {

    constructor(private exp: IValue) {
    }

    get type(): _IType<any> {
        return Types.integer.asArray();
    }

    createGroup(t: _Transaction): AggregationGroupComputer<any[]> {
        let val: any[] = [];
        return {
            feedItem: (item) => {
                const value = this.exp.get(item, t);
                val = [...val, value];
            },
            finish: () => val,
        }
    }
}

export function buildArrayAgg(this: void, base: _ISelection, call: ExprCall) {
    const args = call.args;
    if (args.length !== 1) {
        throw new QueryError('ARRAY_AGG expects one argument, given ' + args.length);
    }

    const what = buildValue(base, args[0]);
    return new ArrayAggExpr(what);
}
