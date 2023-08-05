import { TransformBase } from './transform-base';
import { _ISelection, _Transaction, IValue, _IIndex, _Explainer, _SelectExplanation, _IType, IndexKey, _ITable, Stats, AggregationComputer, AggregationGroupComputer, setId, _IAggregation } from '../interfaces-private';
import { Expr, ExprRef, ExprCall } from 'pgsql-ast-parser';
import { buildValue } from '../parser/expression-builder';
import { nil, NotSupported } from '../interfaces';
import hash from 'object-hash';
import { Evaluator } from '../evaluator';
import { buildCount } from './aggregations/count';
import { buildMinMax } from './aggregations/max-min';
import { buildSum } from './aggregations/sum';
import { buildArrayAgg } from './aggregations/array_agg';
import { buildAvg } from './aggregations/avg';
import { Selection } from './selection';
import { buildCtx, withSelection } from '../parser/context';
import { buildJsonAgg } from './aggregations/json_aggs';
import { buildStringAgg } from './aggregations/string_agg';
import { nullIsh } from '../utils';
import { buildBoolAgg } from './aggregations/bool-aggregs';

export const aggregationFunctions = new Set([
    'array_agg',
    'avg',
    'bit_and',
    'bit_or',
    'bool_and',
    'bool_or',
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

export function buildGroupBy(on: _ISelection, groupBy: Expr[]) {
    const agg = new Aggregation(on, groupBy);
    return agg;
}

let idCnt = 0;

export function getAggregator(): _IAggregation | null {
    const on = buildCtx().selection;
    if (!on) {
        return null;
    }
    if (on.isAggregation()) {
        return on;
    }
    if (!(on instanceof Selection)) {
        return null;
    }
    if (!(on.base.isAggregation())) {
        return null;
    }
    return on.base;
}

type AggregItem = any;

interface AggregationInstance {
    getter: IValue;
    id: symbol;
    computer: AggregationComputer;
    distinct: IValue[] | nil;
}

function isIntegralType(value: any): boolean {
    return typeof value === 'number' || typeof value === 'string' || value instanceof Date;
}

export class Aggregation<T> extends TransformBase<T> implements _ISelection<T>, _IAggregation {

    columns: readonly IValue<any>[];
    /**
     * Group-by values
     * - key: column in source hash
     * - value: column in this, evaluated against temporary entity.
     **/
    private readonly symbol = Symbol();
    private readonly aggId = idCnt++;
    private readonly groupIndex?: _IIndex<any> | nil;
    private aggregations = new Map<string, AggregationInstance>();

    /** How to get grouping values on the base table raw items (not on "this.enumerate()" raw items)  */
    private groupingValuesOnbase: IValue[];

    /** How to get the grouped values on "this.enumerate()" raw items output */
    private groupByMapping = new Map<string, IValue>();

    isAggregation() {
        return true;
    }

    constructor(on: _ISelection, _groupedBy: Expr[]) {
        super(on);


        // === preassign columns that are reachable (grouped by)
        this.groupingValuesOnbase = withSelection(on, () => _groupedBy.map(x => buildValue(x)));
        for (let _i = 0; _i < this.groupingValuesOnbase.length; _i++) {
            const i = _i;
            const g = this.groupingValuesOnbase[i];
            this.groupByMapping.set(g.hash!, new Evaluator(
                g.type
                , g.id
                , g.hash!
                , [g]
                // keys are stored wrapped in a symbol (because not necessarily selected)
                , v => v[this.symbol][i]
            ));
        }

        // try to find an index matching our groupby clause
        this.groupIndex = on.getIndex(...this.groupingValuesOnbase);
        this.columns = [];
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue<any> | nil {
        return this.base.getColumn(column, nullIfNotFound);
    }

    checkIfIsKey(got: IValue<any>): IValue<any> {
        return this.groupByMapping.get(got.hash!) ?? got;
    }


    entropy(t: _Transaction): number {
        return this.groupByMapping.size || 1;
    }

    stats(): Stats | null {
        // cannot be computed without iterating
        return null;
    }

    *enumerate(t: _Transaction): Iterable<T> {
        for (const item of this._enumerateAggregationKeys(t)) {
            const sym = item[this.symbol];
            setId(item, `agg_${this.aggId}_${hash(sym)}`)
            yield item as any;
        }
    }

    private _enumerateAggregationKeys(t: _Transaction): Iterable<AggregItem> {
        // ===== try to compute directly (will only succeed when no grouping, and simple statements like count(*))
        const ret = this.computeDirect(t);
        if (ret) {
            return [ret];
        }

        // ===== try to compute base on index
        const fromIndex = this.iterateFromIndex(t);
        if (fromIndex) {
            return fromIndex;
        }

        // ==== seq-scan computation
        return this.seqScan(t);
    }

    private iterateFromIndex(t: _Transaction): AggregItem[] | null {
        if (!this.groupIndex) {
            return null;
        }
        const aggs = [...this.aggregations.values()];
        const allByGroup = !aggs.some(x => !x.computer.computeFromIndex);

        if (!allByGroup) {
            return null;
        }

        const indexKeys = this.groupIndex.iterateKeys(t);
        if (!indexKeys) {
            return null;
        }

        // iterate all index keys
        const computed: AggregItem[] = []
        for (const k of indexKeys) {
            const ret: any = { [this.symbol]: k };
            // try to compute from index
            for (const agg of aggs) {
                const val = agg.computer.computeFromIndex?.(k, this.groupIndex, t);
                if (typeof val === 'undefined') {
                    if (computed.length) {
                        throw new Error('Compute from index has succeeded on an index key, but failed on another (which must not happen)');
                    }
                    return null;
                }
                ret[agg.id] = val;
            }
            computed.push(ret);
        }
        return computed;
    }


    private *seqScan(t: _Transaction): Iterable<AggregItem> {
        const aggs = [...this.aggregations.values()];
        const groups = new Map<string, {
            key: IndexKey;
            aggs: {
                computer: AggregationGroupComputer;
                distinctHash: Set<any>;
                instance: AggregationInstance,
            }[];
        }>();

        // === feed all items
        for (const item of this.base.enumerate(t)) {
            // get group-by values
            const key: IndexKey = this.groupingValuesOnbase.map(g => g.get(item, t));

            // add group if necessary
            const groupingKey = hash(key);
            let group = groups.get(groupingKey);
            if (!group) {
                groups.set(groupingKey, group = {
                    key,
                    aggs: aggs.map(x => ({
                        computer: x.computer.createGroup(t),
                        instance: x,
                        distinctHash: new Set(),
                    })),
                });
            }

            // process aggregators in group
            for (const g of group.aggs) {
                if (!g.computer) {
                    continue;
                }
                if (g.instance.distinct) {
                    const distinctValues = g.instance.distinct.map(x => x.get(item, t));
                    if (distinctValues.length === 1 && nullIsh(distinctValues[0])) {
                        // ignore single nulls.
                        continue;
                    }
                    let valuesHash: any;
                    if (distinctValues.length === 1 && isIntegralType(distinctValues[0])) {
                        // optimization to avoid hashing on objects supported by "Set"
                        valuesHash = distinctValues[0];
                    } else {
                        valuesHash = hash(distinctValues);
                    }
                    if (g.distinctHash.has(valuesHash)) {
                        continue;
                    }
                    g.distinctHash.add(valuesHash);
                }
                g.computer.feedItem(item);
            }
        }

        // if this.base is empty, and this is not a group by...
        //  👉 Must return a result.
        //   ex:
        //      - select max(a) from empty              👉 [{max: null}]
        //      - select max(a) from empty group by id  👉 []
        if (groups.size === 0 && !this.groupingValuesOnbase.length) {
            const key: IndexKey = [];
            const groupingKey = hash(key);
            groups.set(groupingKey, {
                key,
                aggs: aggs.map(x => ({
                    computer: x.computer.createGroup(t),
                    instance: x,
                    distinctHash: new Set(),
                })),
            });
        }

        // === return results
        for (const g of groups.values()) {
            const ret: AggregItem = {
                // specify the grouping key
                [this.symbol]: g.key
            };

            // fill aggregations values
            for (const { instance: { id }, computer } of g.aggs) {
                ret[id] = computer.finish() ?? null;
            }
            yield ret;
        }
    }

    private computeDirect(t: _Transaction): AggregItem | null {
        // When there is no grouping...
        if (this.groupByMapping.size) {
            return null;
        }
        // check if all selected aggregations can be computed directly (typically: count(*))
        const aggs = [...this.aggregations.values()];
        const allNoGroup = !aggs.some(x => !x.computer.computeNoGroup);
        if (!allNoGroup) {
            return null;
        }
        const ret: AggregItem = {
            [this.symbol]: [],
        };
        for (const agg of this.aggregations.values()) {
            const val = agg.computer.computeNoGroup?.(t);
            if (typeof val === 'undefined') {
                return null;
            }
            ret[agg.id] = val;
        }
        return ret;
    }

    getAggregation(name: string, call: ExprCall): IValue {
        const hashed = hash(call);
        const agg = this.aggregations.get(hashed);
        if (agg) {
            return agg.getter;
        }
        const got = this._getAggregation(name, call);

        const id = Symbol(name);
        const getter = new Evaluator(
            got.type
            , null
            , hashed
            , []
            , raw => raw[id]
            , {
                forceNotConstant: true,
            });

        let distinct: IValue[] | null = null;
        if (call.distinct === 'distinct') {
            if (call.args.length === 1 && call.args[0].type === 'list') {
                // hack in case we get a record-like thing - ex: select count(distinct (a,b))
                // cf UT behaves nicely with nulls on multiple count
                distinct = call.args[0].expressions.map(x => buildValue(x));
            } else {
                distinct = call.args.map(x => buildValue(x));
            }
        }

        this.aggregations.set(hashed, {
            id,
            getter,
            computer: got,
            distinct,
        });
        return getter;
    }

    private _getAggregation(name: string, call: ExprCall): AggregationComputer {
        switch (name) {
            case 'count':
                return buildCount(this.base, call);
            case 'max':
            case 'min':
                return buildMinMax(this.base, call.args, name);
            case 'sum':
                return buildSum(this.base, call);
            case 'array_agg':
                return buildArrayAgg(this.base, call);
            case 'avg':
                return buildAvg(this.base, call);
            case 'jsonb_agg':
            case 'json_agg':
                return buildJsonAgg(this.base, call, name);
            case 'string_agg':
                return buildStringAgg(this.base, call, name);
            case 'bool_and':
            case 'bool_or':
                return buildBoolAgg(this.base, call, name);
            default:
                throw new NotSupported('aggregation function ' + name);
        }
    }


    hasItem(value: T, t: _Transaction): boolean {
        return !!(value as any)[this.symbol];
    }

    getIndex(forValue: IValue<any>): _IIndex<any> | nil {
        // there is no index on aggregations
        return null;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            _: 'aggregate',
            id: e.idFor(this),
            aggregator: null as any,
        }
    }

}
