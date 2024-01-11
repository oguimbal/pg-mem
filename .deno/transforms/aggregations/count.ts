import { AggregationComputer, AggregationGroupComputer, IndexKey, IValue, QueryError, _IIndex, _ISelection, _IType, _Transaction } from '../../interfaces-private.ts';
import { ExprCall } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { isSelectAllArgList, nullIsh } from '../../utils.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { Types } from '../../datatypes/index.ts';
import { withSelection } from '../../parser/context.ts';

export function buildCount(this: void, base: _ISelection, call: ExprCall) {
    return withSelection(base, () => {
        const args = call.args;
        if (isSelectAllArgList(args)) {
            return new CountStar(base);
        }
        if (args.length !== 1) {
            throw new QueryError('COUNT expects one argument, given ' + args.length);
        }
        const what = buildValue(args[0]);
        return new CountExpr(what);
    });
}

class CountStar implements AggregationComputer<number> {

    constructor(private on: _ISelection) {
    }

    get type(): _IType<any> {
        return Types.bigint;
    }

    computeFromIndex(key: IndexKey, index: _IIndex<any>, t: _Transaction) {
        const stats = index.stats(t, key);
        return stats?.count;
    }

    computeNoGroup(t: _Transaction) {
        return this.on.stats(t)?.count;
    }

    createGroup(): AggregationGroupComputer<number> {
        let cnt = 0;
        return {
            feedItem: () => cnt++,
            finish: () => cnt,
        };
    }

}

class CountExpr implements AggregationComputer<number> {

    constructor(private exp: IValue) {
    }

    get type(): _IType<any> {
        return Types.bigint;
    }

    createGroup(t: _Transaction): AggregationGroupComputer<number> {
        let cnt = 0;
        return {
            feedItem: (item) => {
                const value = this.exp.get(item, t);
                if (!nullIsh(value)) {
                    cnt++;
                }
            },
            finish: () => cnt,
        };
    }
}
