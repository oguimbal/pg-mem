import { AggregationComputer, AggregationGroupComputer, IndexKey, IValue, QueryError, _IIndex, _ISelection, _IType, _Transaction } from '../../interfaces-private';
import { Expr } from 'pgsql-ast-parser';
import { isSelectAllArgList, nullIsh } from '../../utils';
import { buildValue } from '../../expression-builder';
import { Types } from '../../datatypes';
import objectHash from 'object-hash';

export function buildCount(this: void, base: _ISelection, args: Expr[]) {
    if (isSelectAllArgList(args)) {
        return new CountStar(base);
    }
    if (args.length !== 1) {
        throw new QueryError('COUNT expects one argument, given ' + args.length);
    }
    if (args[0].type === 'call') {
        if (args[0].function === 'distinct') {
            if (!args[0].args.length) {
                throw new QueryError('distinct() must take at least one argument');
            }
            const distinctArgs = args[0].args.map(x => buildValue(base, x));
            return new CountDistinct(distinctArgs);
        }
    }
    const what = buildValue(base, args[0]);
    return new CountExpr(what);
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

class CountDistinct implements AggregationComputer<number> {

    constructor(private exps: IValue[]) {
    }

    get type(): _IType<any> {
        return Types.bigint;
    }

    createGroup(t: _Transaction): AggregationGroupComputer<number> {
        const unique = new Set();
        return {
            feedItem: this.exps.length === 1
                ? (item) => {
                    const value = this.exps[0].type.hash(this.exps[0].get(item, t));
                    if (nullIsh(value)) {
                        return; // ignore single nulls.
                    }
                    unique.add(value);
                } : (item) => {
                    const value = this.exps.map(x => x.type.hash(x.get(item, t)));
                    unique.add(objectHash(value));
                },
            finish: () => unique.size,
        };
    }
}
