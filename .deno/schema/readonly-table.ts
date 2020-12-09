import { _ITable, _ISelection, _ISchema, _Transaction, _IIndex, IValue, NotSupported, PermissionDeniedError, _Column, SchemaField, IndexDef, _Explainer, _SelectExplanation, _IType, ChangeHandler, Stats, DropHandler, IndexHandler, RegClass, RegType, Reg } from '../interfaces-private.ts';
import { CreateColumnDef, TableConstraint } from 'https://deno.land/x/pgsql_ast_parser@1.3.5/mod.ts';
import { DataSourceBase } from '../transforms/transform-base.ts';
import { Schema, ColumnNotFound, nil, ISubscription } from '../interfaces.ts';
import { buildAlias } from '../transforms/alias.ts';
import { columnEvaluator } from '../transforms/selection.ts';

export abstract class ReadOnlyTable<T = any> extends DataSourceBase<T> implements _ITable, _ISelection<any> {


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
            this.columnsById.set(_col.name.toLowerCase(), newCol);
        }
    }

    get columns(): ReadonlyArray<IValue<any>> {
        this.build();
        return this._columns!;
    }

    getColumn(column: string): IValue;
    getColumn(column: string, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> | nil {
        this.build();
        const got = this.columnsById.get(column.toLowerCase());
        if (!got && !nullIfNotFound) {
            throw new ColumnNotFound(column);
        }
        return got;
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

    get columnDefs(): _Column[] {
        throw new PermissionDeniedError(this.name);
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
    addConstraint(constraint: TableConstraint, t: _Transaction) {
        throw new PermissionDeniedError(this.name);
    }
    insert(toInsert: any): void {
        throw new PermissionDeniedError(this.name);
    }
    delete(t: _Transaction, toDelete: T): void {
        throw new PermissionDeniedError(this.name);
    }
    createIndex(): this {
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

    onChange(columns: string[], check: ChangeHandler<T>) {
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


    make(table: _ITable, i: number, t: IValue<any>): any {
        throw new Error('not implemented');
    }


    *itemsByTable(table: string | _ITable, t: _Transaction): IterableIterator<any> {
        if (typeof table === 'string') {
            for (const s of this.db.listSchemas()) {
                debugger;
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