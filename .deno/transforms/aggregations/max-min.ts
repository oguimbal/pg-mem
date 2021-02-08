import { AggregationComputer, AggregationGroupComputer, IValue, nil, QueryError, _ISelection, _IType, _Transaction } from '../../interfaces-private.ts';
import { Expr } from 'https://deno.land/x/pgsql_ast_parser@4.2.0/mod.ts';
import { buildValue } from '../../expression-builder.ts';
import { Types } from '../../datatypes/index.ts';
import { nullIsh } from '../../utils.ts';


class MinMax implements AggregationComputer<number> {

    constructor(private exp: IValue, private isMax: boolean) {
    }

    get type(): _IType<any> {
        return Types.bigint;
    }

    createGroup(t: _Transaction): AggregationGroupComputer<number> {
        let val: number | nil = null;
        return {
            feedItem: (item) => {
                const value = this.exp.get(item, t);
                if (!nullIsh(value) && (nullIsh(val) || (
                    this.isMax
                        ? val! < value
                        : val! > value
                ))) {
                    val = value;
                }
            },
            finish: () => val,
        };
    }
}


export function buildMinMax(this: void, base: _ISelection, args: Expr[], op: 'max' | 'min') {
    if (args.length !== 1) {
        throw new QueryError(op.toUpperCase() + ' expects one argument, given ' + args.length);
    }

    const what = buildValue(base, args[0]);
    return new MinMax(what, op === 'max');
}
