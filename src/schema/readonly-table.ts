import { _ITable, _ISelection, _ISchema, _Transaction, _IIndex, IValue, NotSupported, PermissionDeniedError, _Column, SchemaField, IndexDef, _Explainer, _SelectExplanation, _IType, ChangeHandler, Stats, DropHandler, IndexHandler, RegClass, RegType, Reg, _IConstraint, TruncateHandler, Row } from '../interfaces-private';
import { CreateColumnDef, ExprRef, TableConstraint } from 'pgsql-ast-parser';
import { DataSourceBase } from '../transforms/transform-base';
import { Schema, ColumnNotFound, nil, ISubscription, ColumnDef } from '../interfaces';
import { buildAlias } from '../transforms/alias';
import { columnEvaluator } from '../transforms/selection';
import { colByName, findTemplate } from '../utils';
import { cleanResults } from '../execution/clean-results';

export abstract class ReadOnlyTable extends DataSourceBase implements _ITable, _ISelection {


    get isExecutionWithNoResult(): boolean {
        return false;
    }

    get primaryIndex(): nil | IndexDef {
        return null;
    }

    getColumns(): Iterable<ColumnDef> {
        throw new Error('Method not implemented on schema tables.');
    }

    abstract entropy(t: _Transaction): number;
    abstract enumerate(t: _Transaction): Iterable<Row>;
    abstract hasItem(value: Row, t: _Transaction): boolean;
    abstract readonly _schema: Schema;

    reg!: Reg;

    readonly selection: _ISelection = buildAlias(this);
    readonly hidden = true;

    isOriginOf(v: IValue): boolean {
        return v.origin === this || v.origin === this.selection;
    }


    get type() {
        return 'table' as const;
    }

    constructor(private schema: _ISchema) {
        super(schema);
    }

    get name(): string {
        return this._schema.name;
    }

    register() {
        this.reg = this.schema._reg_register(this);
    }

    private columnsById = new Map<string, IValue>();
    private _columns?: IValue[];


    private build() {
        if (this._columns) {
            return;
        }
        this._columns = [];
        let i = 0;
        for (const _col of this._schema.fields) {
            const newCol = this.buildColumnEvaluator(_col, i++);
            this._columns.push(newCol);
            this.columnsById.set(_col.name, newCol);
        }
    }

    protected buildColumnEvaluator(_col: SchemaField, idx: number): IValue {
        return columnEvaluator(this, _col.name, _col.type as _IType);
    }

    get columns(): ReadonlyArray<IValue> {
        this.build();
        return this._columns!;
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil {
        this.build();
        if (typeof column !== 'string'
            && column.table) {
            if (!column.table.schema
                && column.table.name !== this.name) {
                return null;
            }
            column = column.name;
        }
        return colByName(this.columnsById, column, nullIfNotFound);
    }

    explain(e: _Explainer): _SelectExplanation {
        throw new PermissionDeniedError(this.name);
    }

    listIndices(): IndexDef[] {
        return [];
    }

    stats(t: _Transaction): Stats | null {
        throw new NotSupported('stats (count, ...) on information schema');
    }

    rename(to: string): this {
        throw new PermissionDeniedError(this.name);
    }
    update(t: _Transaction, toUpdate: any): never {
        throw new PermissionDeniedError(this.name);
    }
    addColumn(column: SchemaField | CreateColumnDef): _Column {
        throw new PermissionDeniedError(this.name);
    }
    getColumnRef(column: string, nullIfNotFound?: boolean): _Column {
        throw new PermissionDeniedError(this.name);
    }
    getConstraint(constraint: string): _IConstraint | nil {
        return null;
    }
    addConstraint(constraint: TableConstraint, t: _Transaction): _IConstraint {
        throw new PermissionDeniedError(this.name);
    }
    insert(item: any) {
        throw new PermissionDeniedError(this.name);
    }
    doInsert(toInsert: any): void {
        throw new PermissionDeniedError(this.name);
    }
    delete(t: _Transaction, toDelete: Row): void {
        throw new PermissionDeniedError(this.name);
    }
    truncate(t: _Transaction): void {
        throw new PermissionDeniedError(this.name);
    }

    createIndex(): _IConstraint {
        throw new PermissionDeniedError(this.name);
    }
    dropIndex(t: _Transaction, name: string): void {
        throw new PermissionDeniedError(this.name);
    }
    setHidden(): this {
        throw new PermissionDeniedError(this.name);
    }
    drop(t: _Transaction): void {
        throw new PermissionDeniedError(this.name);
    }

    setReadonly(): this {
        return this;
    }

    getIndex(...forValue: IValue[]): _IIndex | nil {
        return null;
    }

    on(): any {
        throw new NotSupported('subscribing information schema');
    }
    onBeforeChange(columns: string[], check: ChangeHandler) {
        // nop
        return { unsubscribe() { } }
    }
    onCheckChange(columns: string[], check: ChangeHandler) {
        // nop
        return { unsubscribe() { } }
    }
    onTruncate(sub: TruncateHandler): ISubscription {
        // nop
        return { unsubscribe() { } }
    }
    onDrop(sub: DropHandler): ISubscription {
        // nop
        return { unsubscribe() { } }
    }
    onIndex(sub: IndexHandler): ISubscription {
        // nop
        return { unsubscribe() { } }
    }


    find(template?: Row, columns?: (keyof Row)[]): Row[] {
        return cleanResults([...findTemplate(this.selection, this.db.data, template, columns)]);
    }


    make(table: _ITable, i: number, t: IValue): any {
        throw new Error('not implemented');
    }


    *itemsByTable(table: string | _ITable, t: _Transaction): IterableIterator<any> {
        if (typeof table === 'string') {
            for (const s of this.db.listSchemas()) {
                const got = s.getTable(table, true);
                if (got) {
                    yield* this.itemsByTable(got, t);
                }
            }
        } else {
            let i = 0;
            for (const f of table.selection.columns) {
                yield this.make(table, ++i, f);
            }
        }
    }
}