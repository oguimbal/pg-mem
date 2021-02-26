import { TransformBase } from './transform-base.ts';
import { _ISelection, _Transaction, IValue, _IIndex, _Explainer, _SelectExplanation, _IType, IndexKey, _ITable, Stats, AggregationComputer, AggregationGroupComputer } from '../interfaces-private.ts';
import { SelectedColumn, Expr, ExprRef } from 'https://deno.land/x/pgsql_ast_parser@6.2.1/mod.ts';
import { buildValue } from '../expression-builder.ts';
import { ColumnNotFound, nil, NotSupported, QueryError } from '../interfaces.ts';
import { colByName, suggestColumnName } from '../utils.ts';
import hash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';
import { Evaluator } from '../evaluator.ts';
import { buildCount } from './aggregations/count.ts';
import { buildMinMax } from './aggregations/max-min.ts';
import { buildSum } from './aggregations/sum.ts';

export const aggregationFunctions = new Set([
    'array_agg',
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

export function buildGroupBy(on: _ISelection, groupBy: Expr[], select: SelectedColumn[]) {
    const group = groupBy.map(x => buildValue(on, x));
    return new Aggregation(on, group, select);
}

export class Aggregation<T> extends TransformBase<T> implements _ISelection<T> {

    columns: readonly IValue<any>[];
    /**
     * Group-by values
     * - key: column in source hash
     * - value: column in this, evaluated against temporary entity.
     **/
    private groupByMapping = new Map<string, IValue>();
    private building: 'groupby' | 'select' | null = 'groupby';
    private readonly symbol = Symbol();
    private readonly groupIndex?: _IIndex<any> | nil;
    private columnsById: Map<string, IValue>;
    private aggregations = new Map<string, {
        getter: IValue;
        id: symbol;
        computer: AggregationComputer;
    }>();

    constructor(on: _ISelection, private readonly groupedBy: IValue[], select: SelectedColumn[]) {
        super(on);

        // preassign columns that are reachable (grouped by)
        for (let _i = 0; _i < groupedBy.length; _i++) {
            const i = _i;
            const g = groupedBy[i];
            this.groupByMapping.set(g.hash!, new Evaluator(
                on.ownerSchema
                , g.type
                , g.id
                , g.hash!
                , [g]
                , v => v[this.symbol][i]
            ));
        }

        // try to find an index matching our groupby clause
        this.groupIndex = on.getIndex(...groupedBy);

        // build selection
        const cols = new Map<string, IValue>();
        let anonymous = 0;
        this.building = 'select';
        for (const s of select) {
            const built = buildValue(this, s.expr);
            if (s.alias && cols.has(s.alias.name)) {
                throw new NotSupported('Ambiguous aliasing');
            }
            const name = s.alias?.name
                ?? suggestColumnName(s.expr)
                ?? built.id
                ?? ('column' + (anonymous++));
            cols.set(name, built);
        }
        this.building = null;
        this.columnsById = cols;
        this.columns = [...cols.values()];
    }


    entropy(t: _Transaction): number {
        return this.groupByMapping.size || 1;
    }

    stats(): Stats | null {
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
                for (const k of this.groupIndex.iterateKeys(t)!) {
                    if (invalid) {
                        break;
                    }
                    const ret: any = { [this.symbol]: k };
                    // try to compute from index
                    for (const agg of aggs) {
                        const val = agg.computer.computeFromIndex?.(k, this.groupIndex, t);
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
        const groups = new Map<string, {
            key: IndexKey;
            aggs: {
                id: symbol;
                computer: AggregationGroupComputer;
            }[];
        }>();
        // === feed all items
        for (const item of this.base.enumerate(t)) {
            const key: IndexKey = this.groupedBy.map(g => g.get(item, t));
            const groupingKey = hash(key);
            let group = groups.get(groupingKey);
            if (!group) {
                groups.set(groupingKey, group = {
                    key,
                    aggs: aggs.map(x => ({
                        id: x.id,
                        computer: x.computer.createGroup(t),
                    })),
                });
            }
            for (const g of group.aggs) {
                g.computer.feedItem(item);
            }
        }
        // if this.base is empty
        if (groups.size === 0) {
            const key: IndexKey = this.groupedBy.map(() => null);
            const groupingKey = hash(key);
            groups.set(groupingKey, {
                key,
                aggs: aggs.map(x => ({
                    id: x.id,
                    computer: x.computer.createGroup(t),
                })),
            });
        }

        // === return results
        for (const g of groups.values()) {
            const ret: any = { [this.symbol]: g.key };
            for (const { id, computer } of g.aggs) {
                ret[id] = computer.finish() ?? null;
            }
            yield ret;
        }
    }

    computeDirect(t: _Transaction) {
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
        const ret: any = {
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


    checkIfIsKey(got: IValue<any>): IValue<any> {
        if (this.building !== 'select') {
            return got;
        }
        return this.groupByMapping.get(got.hash!) ?? got;
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue<any> | nil {
        if (this.building) {
            // when building, expressions are built agains "this"
            // => must check if parent column exists (might be aliased => cannot check by name)
            return this.base.getColumn(column, nullIfNotFound);
        } else {
            // normal behiavour (get columns from "exterior")
            return colByName(this.columnsById, column, nullIfNotFound);
        }
    }

    getAggregation(name: string, args: Expr[]): IValue {
        const hashed = hash({ name, args });
        const agg = this.aggregations.get(hashed);
        if (agg) {
            return agg.getter;
        }
        const got = this._getAggregation(name, args);

        const id = Symbol();
        const getter = new Evaluator(
            this.ownerSchema
            , got.type
            , null
            , hashed
            , []
            , raw => raw[id]
            , {
                forceNotConstant: true,
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
                return buildCount(this.base, args);
            case 'max':
            case 'min':
                return buildMinMax(this.base, args, name);
            case 'sum':
                return buildSum(this.base, args);
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
