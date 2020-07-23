// <== THERE MUST BE NO ACTUAL IMPORTS OTHER THAN IMPORT TYPES (dependency loop)
// ... use 'kind-of' dependency injection below
import type { _ISelection, IValue, _IIndex, _ISelectionSource, _IQuery, _IDb } from '../interfaces-private';
import type { buildSelection, buildAlias } from './selection';
import type { buildFilter } from './build-filter';
import { Expr, SelectedColumn, SelectStatement } from '../parser/syntax/ast';
import { IQuery } from 'src/interfaces';

interface Fns {
    buildSelection: typeof buildSelection;
    buildAlias: typeof buildAlias;
    buildFilter: typeof buildFilter;
}
let fns: Fns;
export function initialize(init: Fns) {
    fns = init;
}

export abstract class DataSourceBase<T> implements _ISelection<T> {
    abstract enumerate(): Iterable<T>;
    abstract readonly entropy: number;
    abstract readonly columns: IValue<any>[]
    abstract getColumn(column: string, nullIfNotFound?: boolean): IValue<any>;
    abstract hasItem(value: T): boolean;
    abstract getIndex(forValue: IValue): _IIndex<any>;

    constructor(readonly db: _IDb) {
    }

    select(select: SelectedColumn[]): _ISelection<any> {
        return fns.buildSelection(this, select);
    }

    filter(filter: Expr): _ISelection {
        if (!filter) {
            return this;
        }
        const plan = fns.buildFilter(this, filter);
        return plan;
    }


    setAlias(alias?: string): _ISelection<any> {
        return fns.buildAlias(this, alias);
    }


    subquery(data: _ISelection<any>, op: SelectStatement): _ISelection {
        // todo: handle refs to 'data' in op statement.
        return this.db.query.buildSelect(op);
    }
}

export abstract class TransformBase<T, TSel extends _ISelectionSource = _ISelectionSource> extends DataSourceBase<T> {


    constructor(protected base: TSel) {
        super(base.db);
    }

    get entropy(): number {
        return this.base.entropy;
    }

    hasItem(value: T): boolean {
        return this.base.hasItem(value);
    }

    getIndex(forValue: IValue): _IIndex<any> {
        if (!this.base) { // istanbul ignore next
            throw new Error('Should not call .getIndex() on join');
        }
        return this.base.getIndex(forValue);
    }
}

export abstract class FilterBase<T> extends TransformBase<T, _ISelection<T>> {


    constructor(_base: _ISelection<T>) {
        super(_base);
    }

    get columns(): IValue<any>[] {
        return this.base.columns;
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> {
        if (!this.base) { // istanbul ignore next
            throw new Error('Should not call .getColumn() on join');
        }
        if (!('columns' in this.base)) { // istanbul ignore next
            throw new Error('Should not call getColumn() on table');
        }
        return this.base.getColumn(column, nullIfNotFound);
    }
}