// <== THERE MUST BE NO ACTUAL IMPORTS OTHER THAN IMPORT TYPES (dependency loop)
// ... use 'kind-of' dependency injection below
import type { _ISelection, IValue, _IIndex, _ISchema, _IDb, _Transaction, _SelectExplanation, _Explainer, Stats, nil, _IAlias, Row, _IAggregation } from '../interfaces-private';
import type { buildSelection } from './selection';
import type { buildAlias } from './alias';
import type { buildFilter } from './build-filter';
import type { buildGroupBy } from './aggregation';
import type { buildLimit } from './limit';
import type { buildUnion } from './union';
import type { buildOrderBy } from './order-by';
import type { buildDistinct } from './distinct';

import { Expr, SelectedColumn, SelectStatement, LimitStatement, OrderByStatement, ExprRef } from 'pgsql-ast-parser';
import { RestrictiveIndex } from './restrictive-index';

interface Fns {
    buildSelection: typeof buildSelection;
    buildAlias: typeof buildAlias;
    buildLimit: typeof buildLimit;
    buildUnion: typeof buildUnion;
    buildFilter: typeof buildFilter;
    buildGroupBy: typeof buildGroupBy;
    buildOrderBy: typeof buildOrderBy;
    buildDistinct: typeof buildDistinct;
}
let fns: Fns;
export function initialize(init: Fns) {
    fns = init;
}

export abstract class DataSourceBase implements _ISelection {
    abstract enumerate(t: _Transaction): Iterable<Row>;
    abstract entropy(t: _Transaction): number;
    abstract readonly columns: ReadonlyArray<IValue>;
    abstract getColumn(column: string, nullIfNotFound?: boolean): IValue;
    abstract hasItem(value: Row, t: _Transaction): boolean;
    abstract getIndex(forValue: IValue): _IIndex | null | undefined;
    abstract explain(e: _Explainer): _SelectExplanation;
    abstract isOriginOf(a: IValue): boolean;
    abstract stats(t: _Transaction): Stats | null;
    abstract get isExecutionWithNoResult(): boolean;

    isAggregation(): this is _IAggregation {
        return false;
    }
    // abstract get name(): string;


    get db() {
        return this.ownerSchema.db;
    }

    constructor(readonly ownerSchema: _ISchema) {
    }

    listColumns(): Iterable<IValue> {
        return this.columns;
    }

    listSelectableIdentities(): Iterable<IValue> {
        return this.columns;
    }

    select(select: (string | SelectedColumn)[] | nil): _ISelection {
        let sel: SelectedColumn[] | nil;
        if (select?.some(v => typeof v === 'string')) {
            sel = select.map<SelectedColumn>(v => typeof v !== 'string'
                ? v
                : {
                    expr: { type: 'ref', name: v },
                })
        } else {
            sel = select as SelectedColumn[] | nil;
        }
        return fns.buildSelection(this, sel);
    }


    selectAlias(alias: string): _IAlias | nil {
        return null;
    }


    filter(filter: Expr | undefined | null): _ISelection {
        if (!filter) {
            return this;
        }
        const plan = fns.buildFilter(this, filter, 'WHERE');
        return plan;
    }

    groupBy(grouping: Expr[] | nil): _ISelection {
        if (!grouping?.length) {
            return this;
        }
        const plan = fns.buildGroupBy(this, grouping);
        return plan;
    }


    setAlias(alias?: string): _ISelection {
        return fns.buildAlias(this, alias);
    }

    limit(limit: LimitStatement): _ISelection {
        if (!limit?.limit && !limit?.offset) {
            return this;
        }
        return fns.buildLimit(this, limit)
    }

    orderBy(orderBy: OrderByStatement[] | nil): _ISelection {
        if (!orderBy?.length) {
            return this;
        }
        return fns.buildOrderBy(this, orderBy);
    }

    distinct(exprs?: Expr[]): _ISelection {
        return fns.buildDistinct(this, exprs);
    }

    union(right: _ISelection): _ISelection {
        return fns.buildUnion(this, right);
    }

}


export abstract class TransformBase extends DataSourceBase {


    constructor(readonly base: _ISelection) {
        super(base.ownerSchema);
    }

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    entropy(t: _Transaction): number {
        return this.base.entropy(t);
    }

    isOriginOf(a: IValue): boolean {
        return a.origin === this || this.base?.isOriginOf(a);
    }
}

export abstract class FilterBase extends TransformBase {

    isAggregation(): this is _IAggregation {
        return false;
    }

    constructor(_base: _ISelection) {
        super(_base);
    }

    get columns(): ReadonlyArray<IValue> {
        return this.base.columns;
    }

    selectAlias(alias: string): nil | _IAlias {
        // this is a filter... that the alias returned by the unfiltered datasource will
        // be valid for the filtered datasource (aliases are only listing columns)
        return this.base.selectAlias(alias);
    }

    /**
    private _columns: IValue[];
    private _columnMappings: Map<IValue, IValue>;
    get columns(): ReadonlyArray<IValue> {
        this.initCols();
        return this._columns;
        // return this.base.columns;
    }

    private initCols() {
        if (this._columns) {
            return;
        }
        this._columns = [];
        this._columnMappings = new Map();
        for (const c of this.base.columns) {
            const nc = c.setOrigin(this);
            this._columns.push(nc);
            this._columnMappings.set(c, nc);
        }
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue {
        if (!this.base) { // istanbul ignore next
            throw new Error('Should not call .getColumn() on join');
        }
        if (!('columns' in this.base)) { // istanbul ignore next
            throw new Error('Should not call getColumn() on table');
        }
        this.initCols();
        const col = this.base.getColumn(column, nullIfNotFound);
        return col && this._columnMappings.get(col);
    }
     */

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil {
        if (!this.base) { // istanbul ignore next
            throw new Error('Should not call .getColumn() on join');
        }
        if (!('columns' in this.base)) { // istanbul ignore next
            throw new Error('Should not call getColumn() on table');
        }
        return this.base.getColumn(column, nullIfNotFound);
    }

    getIndex(...forValue: IValue[]): _IIndex | null | undefined {
        const index = this.base.getIndex(...forValue);
        if (!index) {
            return null;
        }
        return new RestrictiveIndex(index, this);
    }
}
