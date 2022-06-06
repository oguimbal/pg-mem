import { _ISelection, QueryError, AggregationComputer, IValue, _IType, _Transaction, AggregationGroupComputer } from '../../interfaces-private';
import { ExprCall } from 'pgsql-ast-parser';
import { withSelection } from '../../parser/context';
import { buildValue } from '../../parser/expression-builder';
import { nullIsh } from '../../utils';
import { Types } from '../../datatypes';


class JsonAggExpr implements AggregationComputer<any[]> {

    constructor(private exp: IValue, readonly type: _IType) {
    }

    createGroup(t: _Transaction): AggregationGroupComputer<any[]> {
        let full: any[][] = [];
        return {
            feedItem: (item) => {
                const value = this.exp.get(item, t);
                if (!nullIsh(value)) {
                    full.push(value);
                }
            },
            finish: () => full.length === 0 ? null : full,
        }
    }
}


export function buildJsonAgg(this: void, base: _ISelection, call: ExprCall, fn: 'json_agg' | 'jsonb_agg') {
    return withSelection(base, () => {
        const args = call.args;
        if (args.length !== 1) {
            throw new QueryError(fn + ' expects one argument, given ' + args.length);
        }
        const type = fn === 'json_agg' ? Types.json : Types.jsonb;
        const what = buildValue(args[0]);
        return new JsonAggExpr(what, type);
    });
}
