import { AggregationComputer, AggregationGroupComputer, IValue, nil, QueryError, _ISelection, _IType, _Transaction } from '../../interfaces-private';
import { ExprCall } from 'pgsql-ast-parser';
import { buildValue } from '../../parser/expression-builder';
import { Types } from '../../datatypes';
import { withSelection } from '../../parser/context';


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
    return withSelection(base, () => {
        const args = call.args;
        if (args.length !== 1) {
            throw new QueryError('ARRAY_AGG expects one argument, given ' + args.length);
        }

        const what = buildValue(args[0]);
        return new ArrayAggExpr(what);
    })
}
