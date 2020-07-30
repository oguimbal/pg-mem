import { _ITable, _ISelection, _ISchema, _Transaction, _IIndex, IValue, NotSupported, ReadOnlyError, _Column, SchemaField, IndexDef, _Explainer, _SelectExplanation, _IType, ChangeHandler, Stats } from '../interfaces-private';
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

    isOriginOf(v: IValue) {
        return v.origin === this || v.origin === this.selection;
    }

    constructor(schema: _ISchema) {
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
            const newCol = columnEvaluator(this, _col.name, _col.type as _IType);
            this._columns.push(newCol);
            this.columnsById.set(_col.name.toLowerCase(), newCol);
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

    listIndices(): IndexDef[] {
        return [];
    }

    stats(t: _Transaction): Stats | null {
        throw new NotSupported('stats (count, ...) on information schema');
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
    addColumn(column: SchemaField | CreateColumnDef): _Column {
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
    delete(t: _Transaction, toDelete: T): void {
        throw new ReadOnlyError('information schema');
    }
    createIndex(): this {
        throw new ReadOnlyError('information schema');
    }

    setReadonly(): this {
        return this;
    }

    getIndex(...forValue: IValue[]): _IIndex<any> {
        return null;
    }

    on(): any {
        throw new NotSupported('subscribing information schema');
    }

    onChange(columns: string[], check: ChangeHandler<T>) {
        // nop
    }

}