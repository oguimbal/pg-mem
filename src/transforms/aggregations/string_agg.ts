import { AggregationComputer, AggregationGroupComputer, IValue, QueryError, _ISelection, _IType, _Transaction } from '../../interfaces-private';
import { ExprCall } from 'pgsql-ast-parser';
import { buildValue } from '../../parser/expression-builder';
import { Types } from '../../datatypes';
import { withSelection } from '../../parser/context';
import { nullIsh } from '../../utils';

class StringAggExpr implements AggregationComputer<string> {

    constructor(private exp: IValue, private delimiter: IValue) {
    }

    get type(): _IType<any> {
        return Types.text();
    }

    createGroup(t: _Transaction): AggregationGroupComputer<string> {
        const parts: string[] = [];
        let delim: string | null = null;
        return {
            feedItem: (item) => {
                const v = this.exp.get(item, t);
                if (!nullIsh(v)) {
                    parts.push(String(v));
                }
                if (delim === null) {
                    const d = this.delimiter.get(item, t);
                    delim = nullIsh(d) ? '' : String(d);
                }
            },
            // string_agg over an empty group is null (like other aggregates)
            finish: () => parts.length ? parts.join(delim ?? '') : null as any,
        };
    }
}

export function buildStringAgg(this: void, base: _ISelection, call: ExprCall) {
    return withSelection(base, () => {
        if (call.args.length !== 2) {
            throw new QueryError('string_agg expects two arguments, given ' + call.args.length);
        }
        return new StringAggExpr(buildValue(call.args[0]), buildValue(call.args[1]));
    });
}
