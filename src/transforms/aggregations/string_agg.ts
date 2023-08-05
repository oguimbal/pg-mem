import { _ISelection, QueryError, AggregationComputer, IValue, _IType, _Transaction, AggregationGroupComputer } from '../../interfaces-private';
import { ExprCall } from 'pgsql-ast-parser';
import { withSelection } from '../../parser/context';
import { buildValue } from '../../parser/expression-builder';
import { Types } from '../../datatypes';


class StringAgg implements AggregationComputer<string> {

    constructor(private exp: IValue, private separator: string) {
    }

    get type(): _IType<string> {
        return Types.citext;
    }

    createGroup(t: _Transaction): AggregationGroupComputer<any[string]> {
        let val: any[string] = [];
        return {
            feedItem: (item) => {
                const value = this.exp.get(item, t);
                val = [...val, value]
            },
            finish: () => val.join(this.separator),
        }
    }
}



export function buildStringAgg(this: void, base: _ISelection, call: ExprCall, separator: string) {
    return withSelection(base, () => {
        const args = call.args;
        if (args.length !== 1) {
            throw new QueryError('string_agg expects one argument, given ' + args.length);
        }
        const what = buildValue(args[0]);
        return new StringAgg(what, separator);
    });
}
