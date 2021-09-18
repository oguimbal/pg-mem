// <== THERE MUST BE NO ACTUAL IMPORTS OTHER THAN IMPORT TYPES (dependency loop)
// ... use 'kind-of' dependency injection below
import type { _ISelection, IValue, _IIndex, _ISchema, _IDb, _Transaction, _SelectExplanation, _Explainer, Stats, nil, _IAlias } from '../interfaces-private';
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

export abstract class DataSourceBase<T> implements _ISelection<T> {
    abstract enumerate(t: _Transaction): Iterable<T>;
    abstract entropy(t: _Transaction): number;
    abstract readonly columns: ReadonlyArray<IValue<any>>;
    abstract getColumn(column: string, nullIfNotFound?: boolean): IValue<any>;
    abstract hasItem(value: T, t: _Transaction): boolean;
    abstract getIndex(forValue: IValue): _IIndex<any> | null | undefined;
    abstract explain(e: _Explainer): _SelectExplanation;
    abstract isOriginOf(a: IValue<any>): boolean;
    abstract stats(t: _Transaction): Stats | null;
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

    select(select: (string | SelectedColumn)[] | nil): _ISelection<any> {
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
        return this;
    }


    filter(filter: Expr | undefined | null): _ISelection {
        if (!filter) {
            return this;
        }
        const plan = fns.buildFilter(this, filter);
        return plan;
    }

    groupBy(grouping: Expr[] | nil, select: SelectedColumn[]): _ISelection {
        if (!grouping?.length) {
            return this;
        }
        const plan = fns.buildGroupBy(this, grouping, select);
        return plan;
    }


    setAlias(alias?: string): _ISelection<any> {
        return fns.buildAlias(this, alias);
    }


    subquery(data: _ISelection<any>, op: SelectStatement): _ISelection {
        // todo: handle refs to 'data' in op statement.
        return this.ownerSchema.buildSelect(op);
    }

    limit(limit: LimitStatement): _ISelection {
        if (!limit?.limit && !limit?.offset) {
            return this;
        }
        return fns.buildLimit(this, limit)
    }

    orderBy(orderBy: OrderByStatement[] | nil): _ISelection<any> {
        if (!orderBy?.length) {
            return this;
        }
        return fns.buildOrderBy(this, orderBy);
    }

    distinct(exprs?: Expr[]): _ISelection<any> {
        return fns.buildDistinct(this, exprs);
    }

    union(right: _ISelection<any>): _ISelection<any> {
        return fns.buildUnion(this, right);
    }

}

export abstract class TransformBase<T> extends DataSourceBase<T> {


    constructor(readonly base: _ISelection) {
        super(base.ownerSchema);
    }

    entropy(t: _Transaction): number {
        return this.base.entropy(t);
    }

    isOriginOf(a: IValue<any>): boolean {
        return a.origin === this || this.base?.isOriginOf(a);
    }
}

export abstract class FilterBase<T> extends TransformBase<T> {


    constructor(_base: _ISelection<T>) {
        super(_base);
    }

    get columns(): ReadonlyArray<IValue<any>> {
        return this.base.columns;
    }

    /**
    private _columns: IValue[];
    private _columnMappings: Map<IValue, IValue>;
    get columns(): ReadonlyArray<IValue<any>> {
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

    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> {
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
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue<any> | nil {
        if (!this.base) { // istanbul ignore next
            throw new Error('Should not call .getColumn() on join');
        }
        if (!('columns' in this.base)) { // istanbul ignore next
            throw new Error('Should not call getColumn() on table');
        }
        return this.base.getColumn(column, nullIfNotFound);
    }

    getIndex(...forValue: IValue<any>[]): _IIndex<any> | null | undefined {
        const index = this.base.getIndex(...forValue);
        if (!index) {
            return null;
        }
        return new RestrictiveIndex(index, this);
    }
}