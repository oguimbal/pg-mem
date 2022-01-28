import { AggregationComputer, AggregationGroupComputer, IValue, nil, QueryError, _ISelection, _IType, _Transaction } from '../../interfaces-private';
import { ExprCall } from 'pgsql-ast-parser';
import { buildValue } from '../../parser/expression-builder';
import { Types } from '../../datatypes';
import { nullIsh, sum } from '../../utils';
import { withSelection } from '../../parser/context';


class AvgExpr implements AggregationComputer<number> {

    constructor(private exp: IValue) {
    }

    get type(): _IType<any> {
        return Types.bigint;
    }

    createGroup(t: _Transaction): AggregationGroupComputer<number> {
        let full: number[] = [];
        return {
            feedItem: (item) => {
                const value = this.exp.get(item, t);
                if (!nullIsh(value)) {
                    full.push(value);
                }
            },
            finish: () => full.length === 0 ? null : sum(full) / full.length,
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
            finish: () => unique.size === 0 ? null : sum([...unique]) / unique.size
        }
    }

}

export function buildAvg(this: void, base: _ISelection, call: ExprCall) {
    return withSelection(base, () => {
        const args = call.args;
        if (args.length !== 1) {
            throw new QueryError('AVG expects one argument, given ' + args.length);
        }

        if (call.distinct) {
            const distinctArg = buildValue(args[0]);
            return new SumDistinct(distinctArg);
        }

        const what = buildValue(args[0]);
        return new AvgExpr(what);
    });
}
