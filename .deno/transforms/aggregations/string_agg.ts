import { AggregationComputer, AggregationGroupComputer, IValue, QueryError, _ISelection, _IType, _Transaction } from '../../interfaces-private.ts';
import { ExprCall } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { Types } from '../../datatypes/index.ts';
import { withSelection } from '../../parser/context.ts';
import { nullIsh } from '../../utils.ts';

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
