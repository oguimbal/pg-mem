import { AggregationComputer, AggregationGroupComputer, IValue, nil, QueryError, _ISelection, _IType, _Transaction } from '../../interfaces-private';
import { ExprCall } from 'pgsql-ast-parser';
import { buildValue } from '../../parser/expression-builder';
import { Types } from '../../datatypes';
import { nullIsh } from '../../utils';


class SumExpr implements AggregationComputer<number> {

    constructor(private exp: IValue) {
    }

    get type(): _IType<any> {
        return Types.bigint;
    }

    createGroup(t: _Transaction): AggregationGroupComputer<number> {
        let val: number | nil = null;
        return {
            feedItem: (item) => {
                const value = this.exp.get(item, t);
                if (!nullIsh(value)) {
                    val = nullIsh(val) ? value : val + value;
                }
            },
            finish: () => val,
        }
    }
}

class SumDistinct implements AggregationComputer<number> {

    constructor(private exp: IValue) {
    }

    get type(): _IType<any> {
        return Types.bigint
    }
    createGroup(t: _Transaction): AggregationGroupComputer<number> {
        const unique = new Set<number>();
        return {
            feedItem: (item) => {
                const value = this.exp.get(item, t)
                if (!nullIsh(value)) {
                    unique.add(value);
                }
            },
            finish: () => unique.size === 0 ? null : [...unique].reduce((acc, cur) => acc + cur, 0)
        }
    }

}

export function buildSum(this: void, base: _ISelection, call: ExprCall) {
    const args = call.args;
    if (args.length !== 1) {
        throw new QueryError('SUM expects one argument, given ' + args.length);
    }

    if (call.distinct) {
        if (args.length !== 1) {
            throw new QueryError('"sum distinct" only takes one argument');
        }
        const distinctArg = buildValue(base, args[0]);
        return new SumDistinct(distinctArg);
    }

    const what = buildValue(base, args[0]);
    return new SumExpr(what);
}
