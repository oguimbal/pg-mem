import { _ISelection, QueryError, AggregationComputer, IValue, _IType, _Transaction, AggregationGroupComputer } from '../../interfaces-private.ts';
import { ExprCall } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { withSelection } from '../../parser/context.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { nullIsh } from '../../utils.ts';
import { Types } from '../../datatypes/index.ts';


class BoolAgg implements AggregationComputer<boolean> {

    constructor(private exp: IValue, private isOr: boolean) {
    }

    get type() {
        return Types.bool;
    }

    createGroup(t: _Transaction): AggregationGroupComputer<boolean> {
        let result: boolean | null = null;
        return {
            feedItem: (item) => {
                if (result === this.isOr) {
                    // no need to compute it further
                    return;
                }
                const value = this.exp.get(item, t);
                if (nullIsh(value)) {
                    return;
                }
                result = !!value;
            },
            finish: () => result,
        }
    }
}



export function buildBoolAgg(this: void, base: _ISelection, call: ExprCall, fn: 'bool_and' | 'bool_or') {
    return withSelection(base, () => {
        const args = call.args;
        if (args.length !== 1) {
            throw new QueryError(fn + ' expects one argument, given ' + args.length);
        }
        const what = buildValue(args[0]).cast(Types.bool);
        return new BoolAgg(what, fn === 'bool_or');
    });
}
