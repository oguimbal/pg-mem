import { IMemoryTable, Schema, SchemaField, DataType, QueryError, RecordExists, TableEvent, ReadOnlyError, NotSupported } from './interfaces';
import { _ISelection, IValue, _ITable, setId, getId, CreateIndexDef, CreateIndexColDef, _IDb, _Transaction, _IQuery, _Column, _IType, CreateColumnDefTyped } from './interfaces-private';
import { buildValue } from './predicate';
import { BIndex } from './btree-index';
import { Selection } from './transforms/selection';
import { parse } from './parser/parser';
import { nullIsh } from './utils';
import { Map as ImMap } from 'immutable';
import { CreateColumnDef, AlterColumn, ColumnConstraint } from './parser/syntax/ast';
import { fromNative } from './datatypes';
import { Evaluator } from './valuetypes';


type Raw<T> = ImMap<string, T>;
export class MemoryTable<T = any> implements IMemoryTable, _ITable<T> {

    private handlers = new Map<TableEvent, Set<() => void>>();

    readonly selection: Selection<T>;
    private it = 0;
    hasPrimary: boolean;
    private readonly: boolean;
    private serials = new Map<string, number>();
    hidden: boolean;
    private dataId = Symbol();
    private indexByHash = new Map<string, BIndex<T>>();
    private indexByName = new Map<string, BIndex<T>>();
    columns: ColRef[] = [];
    columnsByName = new Map<string, ColRef>();

    entropy(t: _Transaction) {
        return this.bin(t).size;
    }

    name: string;

    rename(name: string) {
        const on = this.name;
        if (on === name) {
            return this;
        }
        this.name = name;
        this.schema._doRenTab(on, name);
        return this;
    }

    constructor(readonly schema: _IQuery, t: _Transaction, _schema: Schema) {
        this.name = _schema.name;
        this.selection = new Selection<T>(this, {
            owner: this,
        });

        // fields
        for (const s of _schema.fields) {
            this.addColumn({
                type: s.type as _IType,
                name: s.id,
                constraint: s.primary ? { type: 'primary key' }
                    : s.unique ? { type: 'unique', notNull: s.notNull }
                        : s.notNull ? { type: 'not null' }
                            : null,
                serial: s.autoIncrement,
                default: s.default,
            }, t);
        }

        // auto increments
        for (const s of _schema.fields.filter(x => x.autoIncrement)) {
            this.serials.set(s.id, 0);
        }

        // other table constraints
        for (const c of _schema.constraints ?? []) {
            switch (c.type) {
                case 'primary key':
                    this.createIndex(t, c.columns, 'primary', c.constraintName);
                    break;
                case 'unique':
                    this.createIndex(t, c.columns, 'unique', c.constraintName);
                    break;
                default:
                    throw NotSupported.never(c, 'constraint type');
            }
        }
        this.selection.rebuildColumnIds();
    }

    addColumn(column: CreateColumnDefTyped | CreateColumnDef, t: _Transaction): _Column {
        if ('dataType' in column) {
            const tp = {
                ...column,
                type: fromNative(column.dataType),
            };
            delete tp.dataType;
            return this.addColumn(tp, t);
        }

        const low = column.name.toLowerCase();
        if (this.columnsByName.has(low)) {
            throw new QueryError(`Column "${column.name}" already exists`);
        }
        const cref = new ColRef(this, this.selection.createEvaluator(column.name, column.type));

        if (column.default) {
            cref.alter({
                type: 'set default',
                default: column.default,
                updateExisting: true,
            }, t)
        }

        if (column.constraint) {
            cref.addConstraint(column.constraint, t);
        }

        // once constraints created, reference them. (constraint creation might have thrown)m
        this.selection.addColumn(cref.expression);
        this.columns.push(cref);
        this.columnsByName.set(low, cref);
        this.selection.rebuildColumnIds();
    }


    getColumnRef(column: string, nullIfNotFound?: boolean): _Column {
        const got = this.columnsByName.get(column.toLowerCase());
        if (!got) {
            if (nullIfNotFound) {
                return null;
            }
            throw new QueryError(`Column "${column}" not found`);
        }
        return got;
    }

    bin(t: _Transaction) {
        return t.getMap<Raw<T>>(this.dataId);
    }

    setBin(t: _Transaction, val: Raw<T>) {
        return t.set(this.dataId, val);
    }

    on(event: TableEvent, handler: () => any) {
        let lst = this.handlers.get(event);
        if (!lst) {
            this.handlers.set(event, lst = new Set());
        }
        lst.add(handler);
    }

    raise(event: TableEvent) {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h();
        }
        this.schema.db.raiseTable(this.name, event);
    }

    setReadonly() {
        this.readonly = true;
        return this;
    }
    setHidden() {
        this.hidden = true;
        return this;
    }


    enumerate(t: _Transaction): Iterable<T> {
        this.raise('seq-scan');
        return this.bin(t).values();
    }

    remapData(t: _Transaction, modify: (newCopy: T) => any) {
        // convert raw data (⚠ must copy the whole thing,
        // because it can throw in the middle of this process !)
        //  => this would result in partially converted tables.
        const converted = this.bin(t).map(x => {
            const copy = { ...x };
            modify(copy);
            return copy;
        });
        this.setBin(t, converted);
    }

    insert(t: _Transaction, toInsert: T, shouldHaveId?: boolean): T {
        if (this.readonly) {
            throw new ReadOnlyError(this.name);
        }

        // get ID of this item
        let newId: string;
        if (shouldHaveId) {
            newId = getId(toInsert);
            if (!newId) {
                throw new Error('Unexpeced update error');
            }
        } else {
            newId = this.name + '_' + (this.it++);
            setId(toInsert, newId);
        }

        // serial (auto increments) columns
        for (const [k, v] of this.serials.entries()) {
            if (!nullIsh(toInsert[k])) {
                continue;
            }
            toInsert[k] = v + 1;
            this.serials.set(k, v + 1);
        }

        // index & check indx contrainsts
        this.indexElt(t, toInsert);

        // set default values
        for (const c of this.columns) {
            c.setDefaults(toInsert, t);
        }

        // check constraints
        for (const c of this.columns) {
            c.checkConstraints(toInsert, t);
        }

        this.setBin(t, this.bin(t).set(newId, toInsert));
        return toInsert;
    }

    update(t: _Transaction, toUpdate: T): T {
        if (this.readonly) {
            throw new ReadOnlyError(this.name);
        }
        this.delete(t, toUpdate);
        return this.insert(t, toUpdate, true);
    }

    delete(t: _Transaction, toDelete: T) {
        const id = getId(toDelete);
        const bin = this.bin(t);
        const got = bin.get(id);
        if (!id || !got) {
            throw new Error('Unexpected error: an operation has been asked on an item which does not belong to this table');
        }

        // remove from indices
        for (const k of this.indexByHash.values()) {
            k.delete(got, t);
        }
        this.setBin(t, bin.delete(id));
        return got;
    }


    private indexElt(t: _Transaction, toInsert: T) {
        for (const k of this.indexByHash.values()) {
            k.add(toInsert, t);
        }
    }

    hasItem(item: T, t: _Transaction) {
        const id = getId(item);
        return this.bin(t).has(id);
    }

    getIndex(forValue: IValue) {
        if (forValue.origin !== this.selection) {
            return null;
        }
        const got = this.indexByHash.get(forValue.hash);
        return got ?? null;
    }


    createIndex(t: _Transaction, expressions: string[] | CreateIndexDef, type?: 'primary' | 'unique', indexName?: string): this {
        if (this.readonly) {
            throw new ReadOnlyError(this.name);
        }
        if (Array.isArray(expressions)) {
            const keys: CreateIndexColDef[] = [];
            for (const e of expressions) {
                const parsed = parse(e, 'expr');
                const getter = buildValue(this.selection, parsed);
                keys.push({
                    value: getter,
                });
            }
            return this.createIndex(t, {
                columns: keys,
                primary: type === 'primary',
                notNull: type === 'primary',
                unique: !!type,
            });
        }

        if (!expressions?.columns?.length) {
            throw new QueryError('Empty index');
        }

        if (expressions.primary && this.hasPrimary) {
            throw new QueryError('Table ' + this.name + ' already has a primary key');
        }
        if (expressions.primary) {
            expressions.notNull = true;
            expressions.unique = true;
        }


        const ihash = expressions.columns.map(x => x.value.hash).sort().join('|');
        const index = new BIndex(t, expressions.columns, this, indexName ?? expressions.indexName ?? ihash, expressions.unique, expressions.notNull);

        if (this.indexByHash.has(ihash) || this.indexByName.has(index.indexName)) {
            throw new QueryError('Index already exists');
        }

        // fill index (might throw if constraint not respected)
        const bin = this.bin(t);
        for (const e of bin.values()) {
            index.add(e, t);
        }

        // =========== reference index ============
        // ⚠⚠ This must be done LAST, to avoid throwing an execption if index population failed
        this.indexByHash.set(ihash, index);
        this.indexByHash.set(index.indexName, index);
        if (expressions.primary) {
            this.hasPrimary = true;
        }
        return this;
    }
}


export class ColRef implements _Column {

    default: IValue;
    notNull: boolean;

    constructor(private table: MemoryTable
        , public expression: Evaluator) {
    }

    addConstraint(constraint: ColumnConstraint, t: _Transaction): this {
        switch (constraint.type) {
            case 'primary key':
                this.table.createIndex(t, {
                    columns: [{ value: this.expression }],
                    primary: true,
                });
                break;
            case 'unique':
                this.table.createIndex(t, {
                    columns: [{ value: this.expression }],
                    notNull: constraint.notNull,
                    unique: true,
                });
                break;
            case 'not null':
                this.addNotNullConstraint(t);
                break;
            default:
                throw NotSupported.never(constraint, 'add constraint type');
        }
        return this;
    }


    private addNotNullConstraint(t: _Transaction) {// check has no null value
        const bin = this.table.bin(t);
        for (const e of bin.values()) {
            const val = this.expression.get(e, t);
            if (nullIsh(val)) {
                throw new QueryError(`Cannot add not null constraint on column "${this.expression.id}": it contains null values`);
            }
        }
        this.notNull = true;
    }

    rename(to: string, t: _Transaction): this {
        if (this.table.getColumnRef(to, true)) {
            throw new QueryError(`Column "${to}" already exists`);
        }

        // first, move data (this cannot throw => OK to modify mutable data)
        this.table.remapData(t, v => {
            const ov = v[this.expression.id];
            delete v[this.expression.id];
            v[to] = ov;
        });
        // for (const v of this.table.bin(t)) {
        //     const ov = v[this.expression.id];
        //     delete v[this.expression.id];
        //     v[to] = ov;
        // }

        // === do nasty things to rename column
        this.replaceExpression(to, this.expression.type);
        return this;
    }

    alter(alter: AlterColumn, t: _Transaction): this {
        switch (alter.type) {
            case 'drop default':
                this.default = null;
                break;
            case 'set default':
                const df = buildValue(this.table.selection, alter.default);
                if (!df.isConstant) {
                    throw new QueryError('cannot use column references in default expression');
                }
                if (alter.updateExisting) {
                    const defVal = df.get();
                    this.table.remapData(t, x => x[this.expression.id] = defVal);
                }
                this.default = df;
                break;
            case 'set not null':
                this.addNotNullConstraint(t);
                break;
            case 'drop not null':
                this.notNull = false;
                break;
            case 'set type':
                const newType = fromNative(alter.dataType);
                const conv = this.expression.convert(newType);
                const eid = this.expression.id;

                this.table.remapData(t, x => x[this.expression.id] = conv.get(x, t));

                // once converted, do nasty things to change expression
                this.replaceExpression(eid, newType);
                break;
            default:
                throw NotSupported.never(alter, 'alter column type');
        }
        return this;
    }

    private replaceExpression(newId: string, newType: _IType) {
        const on = this.expression.id.toLowerCase();
        const nn = newId.toLowerCase();
        const i = this.table.selection.columns.indexOf(this.expression);
        if (i < 0) {
            throw new Error('Corrupted table');
        }
        const nexp = this.expression = this.table.selection.createEvaluator(newId, newType);

        // replace in selection
        this.table.selection.columns[i] = nexp;
        this.table.selection.columnsById[nn] = [nexp];

        // replace in table
        this.table.columnsByName.delete(on);
        delete this.table.selection.columnsById[on];
        this.table.columnsByName.set(nn, this);
        this.table.selection.columnsById[nn] = [this.expression];
        this.table.selection.rebuildColumnIds();
    }

    drop(t: _Transaction): void {
        const on = this.expression.id.toLowerCase();
        const i = this.table.selection.columns.indexOf(this.expression);
        const ii = this.table.columns.indexOf(this);
        if (i < 0 || ii !== i) {
            throw new Error('Corrupted table');
        }

        // remove indices

        // remove associated data
        this.table.remapData(t, x => delete x[this.expression.id]);

        // nasty business to remove columns
        this.table.selection.columns.splice(i, 1);
        delete this.table.selection.columnsById[on];
        this.table.selection.rebuildColumnIds();
        this.table.columnsByName.delete(on);
        this.table.columns.splice(i, 1);
    }

    checkConstraints(toInsert: any, t: _Transaction) {
        if (!this.notNull) {
            return;
        }
        const col = this.expression.get(toInsert, t);
        if (nullIsh(col)) {
            throw new QueryError(`null value in column "${this.expression.id}" violates not-null constraint`);
        }
    }

    setDefaults(toInsert: any, t: _Transaction) {
        if (!this.default) {
            return;
        }
        const col = this.expression.get(toInsert, t);
        if (!nullIsh(col)) {
            return;
        }
        toInsert[this.expression.id] = this.default.get();
    }
}