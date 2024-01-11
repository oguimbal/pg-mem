import { TransformBase, FilterBase } from './transform-base.ts';
import { _Transaction, IValue, _Explainer, _ISelection, _SelectExplanation, QueryError, Stats, nil, _IAlias } from '../interfaces-private.ts';
import { Evaluator } from '../evaluator.ts';
import { Types, RecordCol } from '../datatypes/index.ts';
import { ExprRef } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { asSingleName, colToStr } from '../utils.ts';
import { ColumnNotFound } from '../interfaces.ts';
import { RecordType } from '../datatypes/t-record.ts';

export function buildAlias(on: _ISelection, alias?: string): _ISelection {
    if (!alias) {
        return on as any;
    }
    if (on instanceof Alias && on.name === alias) {
        return on;
    }
    return new Alias(on, alias);
}

export class Alias<T> extends TransformBase<T> implements _IAlias {

    private oldToThis = new Map<IValue, IValue>();
    private thisToOld = new Map<IValue, IValue>();
    private _columns: IValue<any>[] | null = null;
    private asRecord!: IValue;

    get isExecutionWithNoResult(): boolean {
        return this.base.isExecutionWithNoResult;
    }

    constructor(sel: _ISelection, public name: string) {
        super(sel);
    }

    *listSelectableIdentities(): Iterable<IValue> {
        this.init();
        yield* super.listSelectableIdentities();
        yield this.asRecord;
    }


    rebuild() {
        this._columns = null;
        this.oldToThis.clear();
        this.thisToOld.clear();
    }

    selectAlias(alias: string): _IAlias | nil {
        if (this.name === alias) {
            return this;
        }
        return null;
    }

    listColumns(): Iterable<IValue> {
        return this.columns;
    }


    get debugId() {
        return this.base.debugId;
    }

    get columns(): ReadonlyArray<IValue<any>> {
        this.init();
        return this._columns!;
    }
    init() {
        if (this._columns) {
            return;
        }
        this._columns = this.base.columns.map(x => {
            const ret = x.setOrigin(this);
            this.oldToThis.set(x, ret);
            this.thisToOld.set(ret, x);
            return ret;
        });

        // how to build a record out of this alias?
        this.asRecord = new Evaluator(
            RecordType.from(this)
            , this.name
            , Math.random().toString()
            , this._columns
            , v => ({ ...v })
            , { forceNotConstant: true });
    }

    stats(t: _Transaction): Stats | null {
        return this.base.stats(t);
    }

    enumerate(t: _Transaction): Iterable<T> {
        return this.base.enumerate(t);
    }

    hasItem(value: T, t: _Transaction): boolean {
        return this.base.hasItem(value, t);
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil {
        const col = this._getColumn(column);
        if (col) {
            return col;
        }

        if (asSingleName(column) === this.name) {
            return this.asRecord;
        }

        if (nullIfNotFound) {
            return null;
        }
        throw new ColumnNotFound(colToStr(column));
    }

    private _getColumn(column: string | ExprRef): IValue | nil {
        if (typeof column !== 'string'
            && column.table) {
            if (!column.table.schema
                && column.table.name !== this.name) {
                return null;
            }
            column = column.name;
        }
        const got = this.base.getColumn(column, true);
        if (!got) {
            return got;
        }
        this.init();
        const ret = this.oldToThis.get(got);
        if (!ret) {
            throw new Error('Corrupted alias');
        }
        return ret;
    }

    explain(e: _Explainer): _SelectExplanation {
        // no need to explain an alias... it does nothing.
        return this.base.explain(e);
        // return {
        //     id: e.idFor(this),
        //     type: 'alias',
        //     alias: this.as,
        //     of: this.base.explain(e),
        // };
    }

    getIndex(...forValue: IValue[]) {
        return this.base.getIndex(...forValue.map(v => this.thisToOld.get(v) ?? v));
    }

}