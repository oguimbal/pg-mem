import { AggregationComputer, AggregationGroupComputer, IndexKey, IValue, QueryError, _IIndex, _ISelection, _IType, _Transaction } from '../../interfaces-private.ts';
import { Expr, ExprCall } from 'https://deno.land/x/pgsql_ast_parser@7.1.0/mod.ts';
import { asSingleQName, isSelectAllArgList, nullIsh } from '../../utils.ts';
import { buildValue } from '../../expression-builder.ts';
import { Types } from '../../datatypes/index.ts';
import objectHash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';

export function buildCount(this: void, base: _ISelection, call: ExprCall) {
    const args = call.args;
    if (isSelectAllArgList(args)) {
        return new CountStar(base);
    }
    if (args.length !== 1) {
        throw new QueryError('COUNT expects one argument, given ' + args.length);
    }
    if (call.distinct) {
        if (!args.length) {
            throw new QueryError('distinct() must take at least one argument');
        }
        if (args.length === 1 && args[0].type === 'list') {
            // hack in case we get a record-like thing - ex: select count(distinct (a,b))
            // cf UT behaves nicely with nulls on multiple count
            const distinctArgs = args[0].expressions.map(x => buildValue(base, x));
            return new CountDistinct(distinctArgs);
        } else {
            const distinctArgs = args.map(x => buildValue(base, x));
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
