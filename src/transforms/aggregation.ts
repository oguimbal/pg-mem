import { TransformBase } from './transform-base';
import { _ISelection, _Transaction, IValue, _IIndex, _Explainer, _SelectExplanation, _IType, IndexKey, _ITable, Stats } from '../interfaces-private';
import { SelectedColumn, Expr } from '../parser/syntax/ast';
import { buildValue } from '../predicate';
import { ColumnNotFound, NotSupported, QueryError } from '../interfaces';
import { isSelectAllArgList, nullIsh } from '../utils';
import hash from 'object-hash';
import { Evaluator } from '../valuetypes';
import { Types } from '../datatypes';

export const aggregationFunctions = new Set([
    'array_agg',
    'array_agg',
    'avg',
    'bit_and',
    'bit_or',
    'bool_and',
    'bool_or',
    'count',
    'count',
    'every',
    'json_agg',
    'jsonb_agg',
    'json_object_agg',
    'jsonb_object_agg',
    'max',
    'min',
    'string_agg',
    'sum',
    'xmlagg',
])

interface AggregationComputer<TRet = any> {
    readonly type: _IType;
    /**  Compute from index  (ex: count(*) with a group-by) */
    computeFromIndex?(key: IndexKey, index: _IIndex, t: _Transaction): TRet;
    /**  Compute out of nowhere when there is no group
     * (ex: when there is no grouping, count(*) on a table or count(xxx) when there is an index on xxx) */
    computeNoGroup?(t: _Transaction): TRet;

    /** When iterating, each new group will have its computer */
    createGroup(t: _Transaction): AggregationGroupComputer<TRet>;
}

interface AggregationGroupComputer<TRet = any> {
    /** When iterating, this will be called for each item in this group  */
    feedItem(item: any): void;
    /** Finish computation (sets aggregation on result) */
    finish(): TRet;
}

export class Aggregation<T> extends TransformBase<T> implements _ISelection<T> {

    columns: readonly IValue<any>[];
    private readonly symbol = Symbol();
    private groupBySet: Set<IValue<any>>;
    private readonly groupIndex: _IIndex<any>;
    private columnsById: Map<string, IValue>;
    private aggregations = new Map<string, {
        getter: IValue;
        id: symbol;
        computer: AggregationComputer;
    }>();

    constructor(on: _ISelection, private readonly groupBy: IValue[], select: SelectedColumn[]) {
        super(on);

        // preassign columns that are reachable (grouped by)
        this.groupBySet = new Set(groupBy);

        // try to find an index matching our groupby clause
        this.groupIndex = on.getIndex(...groupBy);

        // build selection
        const cols = new Map<string, IValue>();
        let anonymous = 0;
        for (const s of select) {
            const built = buildValue(this, s.expr);
            if (s.alias && cols.has(s.alias)) {
                throw new NotSupported('Ambiguous aliasing');
            }
            cols.set(s.alias ?? ('column' + (anonymous++)), built);
        }
        this.columnsById = cols;
        this.columns = [...cols.values()];
    }


    entropy(t: _Transaction): number {
        return this.groupBySet.size || 1;
    }

    stats(): Stats {
        // cannot be computed without iterating
        return null;
    }

    *enumerate(t: _Transaction): Iterable<T> {
        for (const item of this._enumerate(t)) {
            const ret: any = {};
            ret[this.symbol] = true;
            for (const [k, v] of this.columnsById) {
                ret[k] = v.get(item, t);
            }
            yield ret;
        }
    }

    private *_enumerate(t: _Transaction): Iterable<T> {
        const aggs = [...this.aggregations.values()];

        // ===== try to compute directly (will only succeed when no grouping, and simple statements like count(*))
        const ret = this.computeDirect(t);
        if (ret) {
            yield ret;
            return;
        }

        // ===== try to compute base on index
        if (this.groupIndex) {
            const allByGroup = !aggs.some(x => !x.computer.computeFromIndex);
            if (allByGroup) {
                let yielded = false;
                let invalid = false;
                // iterate all index keys
                for (const k of this.groupIndex.iterateKeys(t)) {
                    if (invalid) {
                        break;
                    }
                    const ret: any = {};
                    // try to compute from index
                    for (const agg of aggs) {
                        const val = agg.computer.computeFromIndex(k, this.groupIndex, t);
                        if (typeof val === 'undefined') {
                            if (yielded) {
                                throw new Error('Compute from index has succeeded on an index key, but failed on another (which must not happen)');
                            }
                            invalid = false;
                            break;
                        }
                        ret[agg.id] = val;
                    }
                    yield ret;
                    yielded = true;
                }
                if (!invalid) {
                    return;
                }
            }
        }

        // ==== seq-scan computation
        yield* this.seqScan(t);
    }

    private *seqScan(t: _Transaction): Iterable<any> {
        const aggs = [...this.aggregations.values()];
        const groups = new Map<string, { id: symbol; computer: AggregationGroupComputer }[]>();
        // === feed all items
        for (const item of this.base.enumerate(t)) {
            const groupingKey = hash(this.groupBy.map(g => g.get(item, t)));
            let group = groups.get(groupingKey);
            if (!group) {
                groups.set(groupingKey, group = aggs.map(x => ({
                    id: x.id,
                    computer: x.computer.createGroup(t),
                })));
            }
            for (const g of group) {
                g.computer.feedItem(item);
            }
        }

        // === return results
        for (const g of groups.values()) {
            const ret: any = {};
            for (const { id, computer } of g) {
                ret[id] = computer.finish() ?? null;
            }
            yield ret;
        }
    }

    computeDirect(t: _Transaction) {
        // When there is no grouping...
        if (this.groupBySet.size) {
            return null;
        }
        // check if all selected aggregations can be computed directly (typically: count(*))
        const aggs = [...this.aggregations.values()];
        const allNoGroup = !aggs.some(x => !x.computer.computeNoGroup);
        if (!allNoGroup) {
            return null;
        }
        const ret: any = {};
        for (const agg of this.aggregations.values()) {
            const val = agg.computer.computeNoGroup(t);
            if (typeof val === 'undefined') {
                return null;
            }
            ret[agg.id] = val;
        }
        return ret;
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> {
        const found = this.base.getColumn(column, nullIfNotFound);
        if (!found) {
            return null;
        }
        // only 'groupBy' columns are accessible
        if (!this.groupBySet.has(found)) {
            if (nullIfNotFound) {
                return null;
            }
            throw new ColumnNotFound(column);
        }
        return found;
    }

    getAggregation(name: string, args: Expr[]): IValue {
        const hashed = hash({ name, args });
        const agg = this.aggregations.get(hashed);
        if (agg) {
            return agg.getter;
        }
        const got = this._getAggregation(name, args);

        const id = Symbol();
        const getter = new Evaluator(got.type
            , null
            , name
            , hashed
            , []
            , raw => raw[id]
            , {
                unpure: true
            });

        this.aggregations.set(hashed, {
            id,
            getter,
            computer: got,
        });
        return getter;
    }

    private _getAggregation(name: string, args: Expr[]): AggregationComputer {
        switch (name) {
            case 'count':
                if (isSelectAllArgList(args)) {
                    return new CountStar(this.base);
                }
                if (args.length !== 1) {
                    throw new QueryError('COUNT expects one argument, given ' + args.length);
                }
                const what = buildValue(this.base, args[0]);
                return new CountExpr(what);
            default:
                throw new NotSupported('aggregation function ' + name);
        }
    }


    hasItem(value: T, t: _Transaction): boolean {
        return !!value[this.symbol];
    }

    getIndex(forValue: IValue<any>): _IIndex<any> {
        // there is no index on aggregations
        return null;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            _: 'aggregate',
            id: e.idFor(this),
            aggregator: null,
        }
    }

}


class CountStar implements AggregationComputer<number> {

    constructor(private on: _ISelection) {
    }

    get type(): _IType<any> {
        return Types.long;
    }

    computeFromIndex?(key: IndexKey, index: _IIndex<any>, t: _Transaction) {
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
        return Types.long;
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