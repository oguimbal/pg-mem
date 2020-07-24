import { _ITable, _ISelection, _IQuery, _Transaction, _IIndex, IValue, NotSupported, ReadOnlyError, _Column, CreateColumnDefTyped, IndexDef, _Explainer, _SelectExplanation, _IType } from '../interfaces-private';
import { CreateColumnDef, ConstraintDef } from '../parser/syntax/ast';
import { DataSourceBase } from '../transforms/transform-base';
import { Schema, ColumnNotFound } from '../interfaces';
import { buildAlias } from '../transforms/alias';
import { columnEvaluator } from '../transforms/selection';

export abstract class ReadOnlyTable<T = any> extends DataSourceBase<T> implements _ITable, _ISelection<any> {


    abstract entropy(t: _Transaction): number;
    abstract enumerate(t: _Transaction): Iterable<T>;
    abstract hasItem(value: T, t: _Transaction): boolean;
    abstract readonly _schema: Schema;

    readonly selection: _ISelection = buildAlias(this);
    hidden = true;

    constructor(schema: _IQuery) {
        super(schema);
    }

    private columnsById = new Map<string, IValue>();
    private _columns: IValue[];

    get name(): string {
        return this._schema.name;
    }

    private build() {
        if (this._columns) {
            return;
        }
        this._columns = [];
        for (const _col of this._schema.fields) {
            const newCol = columnEvaluator(this, _col.id, _col.type as _IType);
            this._columns.push(newCol);
            this.columnsById.set(_col.id.toLowerCase(), newCol);
        }
    }

    get columns(): ReadonlyArray<IValue<any>> {
        this.build();
        return this._columns;
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> {
        this.build();
        const got = this.columnsById.get(column.toLowerCase());
        if (!got && !nullIfNotFound) {
            throw new ColumnNotFound(column);
        }
        return got;
    }

    explain(e: _Explainer): _SelectExplanation {
        throw new ReadOnlyError('information schema');
    }


    listIndexes(): IndexDef[] {
        return [];
    }

    get columnDefs(): _Column[] {
        throw new ReadOnlyError('information schema');
    }

    rename(to: string): this {
        throw new ReadOnlyError('information schema');
    }
    update(t: _Transaction, toUpdate: any) {
        throw new ReadOnlyError('information schema');
    }
    addColumn(column: CreateColumnDefTyped | CreateColumnDef): _Column {
        throw new ReadOnlyError('information schema');
    }
    getColumnRef(column: string, nullIfNotFound?: boolean): _Column {
        throw new ReadOnlyError('information schema');
    }
    addConstraint(constraint: ConstraintDef, t: _Transaction) {
        throw new ReadOnlyError('information schema');
    }
    insert(toInsert: any): void {
        throw new ReadOnlyError('information schema');
    }
    createIndex(): this {
        throw new ReadOnlyError('information schema');
    }

    setReadonly(): this {
        return this;
    }

    getIndex(forValue: IValue): _IIndex<any> {
        return null;
    }

    on(): void {
        throw new NotSupported('subscribing information schema');
    }
}