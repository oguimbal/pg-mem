import { _ITable, _ISelection, _ISchema, _Transaction, _IIndex, IValue, NotSupported, PermissionDeniedError, _Column, SchemaField, IndexDef, _Explainer, _SelectExplanation, _IType, ChangeHandler, Stats, DropHandler, IndexHandler, RegClass, RegType, Reg, _IConstraint, TruncateHandler } from '../interfaces-private.ts';
import { CreateColumnDef, ExprRef, TableConstraint } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { DataSourceBase } from '../transforms/transform-base.ts';
import { Schema, ColumnNotFound, nil, ISubscription, ColumnDef } from '../interfaces.ts';
import { buildAlias } from '../transforms/alias.ts';
import { columnEvaluator } from '../transforms/selection.ts';
import { colByName, findTemplate } from '../utils.ts';

export abstract class ReadOnlyTable<T = any> extends DataSourceBase<T> implements _ITable, _ISelection {


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
    abstract enumerate(t: _Transaction): Iterable<T>;
    abstract hasItem(value: T, t: _Transaction): boolean;
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
        for (const _col of this._schema.fields) {
            const newCol = columnEvaluator(this, _col.name, _col.type as _IType);
            this._columns.push(newCol);
            this.columnsById.set(_col.name, newCol);
        }
    }

    get columns(): ReadonlyArray<IValue<any>> {
        this.build();
        return this._columns!;
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue<any> | nil {
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
    update(t: _Transaction, toUpdate: any) {
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
    delete(t: _Transaction, toDelete: T): void {
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

    getIndex(...forValue: IValue[]): _IIndex<any> | nil {
        return null;
    }

    on(): any {
        throw new NotSupported('subscribing information schema');
    }
    onBeforeChange(columns: string[], check: ChangeHandler<T>) {
        // nop
        return { unsubscribe() { } }
    }
    onCheckChange(columns: string[], check: ChangeHandler<T>) {
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


    find(template?: T, columns?: (keyof T)[]): Iterable<T> {
        return findTemplate(this.selection, this.db.data, template, columns);
    }


    make(table: _ITable, i: number, t: IValue<any>): any {
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