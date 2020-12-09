import { IMemoryTable, Schema, QueryError, TableEvent, PermissionDeniedError, NotSupported, IndexDef, ColumnNotFound, ISubscription, nil, DataType } from './interfaces';
import { _ISelection, IValue, _ITable, setId, getId, CreateIndexDef, CreateIndexColDef, _IDb, _Transaction, _ISchema, _Column, _IType, SchemaField, _IIndex, _Explainer, _SelectExplanation, ChangeHandler, Stats, OnConflictHandler, DropHandler, IndexHandler, asIndex, RegClass, RegType, Reg } from './interfaces-private';
import { buildValue } from './predicate';
import { BIndex } from './btree-index';
import { columnEvaluator } from './transforms/selection';
import { nullIsh, deepCloneSimple, Optional } from './utils';
import { Map as ImMap } from 'immutable';
import { CreateColumnDef, TableConstraintForeignKey, TableConstraint, Expr, ExprBinary } from 'pgsql-ast-parser';
import { fromNative } from './datatypes';
import { ColRef } from './column';
import { buildAlias, Alias } from './transforms/alias';
import { DataSourceBase } from './transforms/transform-base';
import { parseSql } from './parse-cache';

function indexHash(this: void, vals: IValue[]) {
    return vals.map(x => x.hash).sort().join('|');
}
type Raw<T> = ImMap<string, T>;
export class MemoryTable<T = any> extends DataSourceBase<T> implements IMemoryTable, _ITable<T> {

    private handlers = new Map<TableEvent, Set<() => void>>();
    readonly selection: Alias<T>;
    readonly reg: Reg;
    private it = 0;
    private cstGen = 0;
    hasPrimary = false;
    private readonly = false;
    hidden = false;
    private dataId = Symbol();
    private serialsId: symbol = Symbol();
    private indexByHash = new Map<string, {
        index: BIndex<T>;
        expressions: IValue[];
    }>();
    columnDefs: ColRef[] = [];
    columnsByName = new Map<string, ColRef>();
    name: string;

    readonly columns: IValue[] = [];
    private changeHandlers = new Set<ChangeHandler<T>>();
    private drophandlers = new Set<DropHandler>();
    private indexHandlers = new Set<IndexHandler>();

    get type() {
        return 'table' as const;
    }

    get debugId() {
        return this.name;
    }

    entropy(t: _Transaction) {
        return this.bin(t).size;
    }

    isOriginOf(a: IValue<any>): boolean {
        return a.origin === this.selection;
    }

    constructor(schema: _ISchema, t: _Transaction, _schema: Schema) {
        super(schema);
        this.name = _schema.name;
        this.reg = schema._reg_register(this);
        this.selection = buildAlias(this, this.name) as Alias<T>;

        // fields
        for (const s of _schema.fields) {
            this.addColumn(s, t);
        }

        // other table constraints
        for (const c of _schema.constraints ?? []) {
            this.addConstraint(c, t);
        }
    }


    stats(t: _Transaction): Stats | null {
        return {
            count: this.bin(t).size,
        };
    }

    rename(name: string) {
        const on = this.name;
        if (on === name) {
            return this;
        }
        this.name = name;
        this.ownerSchema._reg_rename(this, on, name);
        (this.selection as Alias<T>).name = this.name;
        this.db.onSchemaChange();
        return this;
    }

    getColumn(column: string): IValue;
    getColumn(column: string, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> | nil {
        const got = this.columnsByName.get(column.toLowerCase());
        if (!got && !nullIfNotFound) {
            throw new ColumnNotFound(column);
        }
        return got?.expression;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            _: 'table',
            table: this.name,
        };
    }

    addColumn(column: SchemaField | CreateColumnDef, t: _Transaction): _Column {
        if ('dataType' in column) {
            const tp = {
                ...column,
                type: fromNative(column.dataType),
            };
            delete (tp as Optional<typeof tp>).dataType;
            return this.addColumn(tp, t);
        }

        const low = column.name.toLowerCase();
        if (this.columnsByName.has(low)) {
            throw new QueryError(`Column "${column.name}" already exists`);
        }
        const cref = new ColRef(this, columnEvaluator(this.selection, column.name, column.type as _IType), column, column.name);


        // auto increments
        if (column.serial) {
            t.set(this.serialsId, t.getMap(this.serialsId).set(column.name, 0));
        }

        this.columnDefs.push(cref);
        this.columnsByName.set(low, cref);

        try {
            if (column.constraints?.length) {
                cref.addConstraints(column.constraints, t);
            }
            const hasDefault = column.constraints?.some(x => x.type === 'default');
            if (!hasDefault) {
                this.remapData(t, x => (x as any)[column.name] = (x as any)[column.name] ?? null);
            }
        } catch (e) {
            this.columnDefs.pop();
            this.columnsByName.delete(low);
            throw e;
        }

        // once constraints created, reference them. (constraint creation might have thrown)m
        this.columns.push(cref.expression);
        this.db.onSchemaChange();
        this.selection.rebuild();
        return cref;
    }


    getColumnRef(column: string): ColRef;
    getColumnRef(column: string, nullIfNotFound?: boolean): ColRef | nil;
    getColumnRef(column: string, nullIfNotFound?: boolean): ColRef | nil {
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

    on(event: TableEvent, handler: () => any): ISubscription {
        let lst = this.handlers.get(event);
        if (!lst) {
            this.handlers.set(event, lst = new Set());
        }
        lst.add(handler);
        return {
            unsubscribe: () => lst!.delete(handler),
        };
    }

    raise(event: TableEvent) {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h();
        }
        this.db.raiseTable(this.name, event);
    }

    setReadonly() {
        this.readonly = true;
        return this;
    }
    setHidden() {
        this.hidden = true;
        return this;
    }


    *enumerate(t: _Transaction): Iterable<T> {
        this.raise('seq-scan');
        for (const v of this.bin(t).values()) {
            yield deepCloneSimple(v); // copy the original data to prevent it from being mutated.
        }
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

    insert(t: _Transaction, toInsert: T, onConflict?: OnConflictHandler): T {
        if (this.readonly) {
            throw new PermissionDeniedError(this.name);
        }

        // get ID of this item
        const newId = this.name + '_' + (this.it++);
        setId(toInsert, newId);

        // serial (auto increments) columns
        let serials = t.getMap(this.serialsId);
        for (const [k, v] of serials.entries()) {
            if (!nullIsh((toInsert as any)[k])) {
                continue;
            }
            (toInsert as any)[k] = v + 1;
            serials = serials.set(k, v + 1);
        }
        t.set(this.serialsId, serials);

        // set default values
        for (const c of this.columnDefs) {
            c.setDefaults(toInsert, t);
        }

        // check constraints
        for (const c of this.columnDefs) {
            c.checkConstraints(toInsert, t);
        }

        // check "on conflict"
        if (onConflict) {
            if ('ignore' in onConflict) {
                if (onConflict.ignore === 'all') {
                    for (const k of this.indexByHash.values()) {
                        const found = k.index.eqFirst(k.index.buildKey(toInsert, t), t);
                        if (found) {
                            return found; // ignore.
                        }
                    }
                } else {
                    const index = onConflict.ignore as BIndex;
                    const found = index.eqFirst(index.buildKey(toInsert, t), t);
                    if (found) {
                        return found; // ignore.
                    }
                }
            } else {
                const index = onConflict.onIndex as BIndex;
                const key = index.buildKey(toInsert, t);
                const got = index.eqFirst(key, t);
                if (got) {
                    // update !
                    onConflict.update(got, toInsert);
                    return this.update(t, got);
                }
            }
        }

        // check change handlers (foreign keys)
        for (const h of this.changeHandlers) {
            h(null, toInsert, t);
        }

        // index & check indx contrainsts
        this.indexElt(t, toInsert);
        this.setBin(t, this.bin(t).set(newId, toInsert));
        return toInsert;
    }

    update(t: _Transaction, toUpdate: T): T {
        if (this.readonly) {
            throw new PermissionDeniedError(this.name);
        }
        const bin = this.bin(t);
        const id = getId(toUpdate);
        const exists = bin.get(id);

        // set default values
        for (const c of this.columnDefs) {
            c.setDefaults(toUpdate, t);
        }

        // check constraints
        for (const c of this.columnDefs) {
            c.checkConstraints(toUpdate, t);
        }


        // check change handlers (foreign keys)
        if (exists && this.changeHandlers.size) {
            const change = new Set<ChangeHandler<T>>();
            for (const c of this.columnDefs.filter(x => x.changeHandlers.size)) {
                const old = (exists as any)[c.expression.id!];
                const neu = (toUpdate as any)[c.expression.id!];
                if (c.expression.type.equals(old, neu)) {
                    continue;
                }
                for (const ch of c.changeHandlers) {
                    change.add(ch);
                }
                if (change.size === this.changeHandlers.size) {
                    break;
                }
            }
            for (const ch of change) {
                ch(exists, toUpdate, t); // actual check
            }
        }

        // remove old version from index
        if (exists) {
            for (const k of this.indexByHash.values()) {
                k.index.delete(exists, t);
            }
        }

        // add new version to index
        this.indexElt(t, toUpdate);

        // store raw
        this.setBin(t, bin.delete(id).set(id, toUpdate));
        return toUpdate;
    }

    delete(t: _Transaction, toDelete: T) {
        const id = getId(toDelete);
        const bin = this.bin(t);
        const got = bin.get(id);
        if (!id || !got) {
            throw new Error('Unexpected error: an operation has been asked on an item which does not belong to this table');
        }

        // check change handlers (foreign keys)
        for (const h of this.changeHandlers) {
            h(toDelete, null, t);
        }

        // remove from indices
        for (const k of this.indexByHash.values()) {
            k.index.delete(got, t);
        }
        this.setBin(t, bin.delete(id));

        return got;
    }


    private indexElt(t: _Transaction, toInsert: T) {
        for (const k of this.indexByHash.values()) {
            k.index.add(toInsert, t);
        }
    }

    hasItem(item: T, t: _Transaction) {
        const id = getId(item);
        return this.bin(t).has(id);
    }

    getIndex(...forValues: IValue[]): _IIndex | nil {
        if (!forValues.length || forValues.some(x => !x || !this.isOriginOf(x))) {
            return null;
        }
        const ihash = indexHash(forValues);
        const got = this.indexByHash.get(ihash);
        return got?.index ?? null;
    }

    constraintNameGen(constraintName?: string) {
        return constraintName
            ?? (this.name + '_constraint_' + (++this.cstGen));
    }

    addCheck(_t: _Transaction, check: Expr, constraintName?: string) {
        constraintName = this.constraintNameGen(constraintName);
        const getter = buildValue(this.selection, check).convert(DataType.bool);

        const checkVal = (t: _Transaction, v: any) => {
            const value = getter.get(v, t);
            if (value === false) {
                throw new QueryError(`check constraint "${constraintName}" is violated by some row`)
            }
        }

        // check that everything matches (before adding check)
        for (const v of this.enumerate(_t)) {
            checkVal(_t, v);
        }

        // add a check for future updates
        this.onChange([], (old, neu, ct) => {
            if (!neu) {
                return;
            }
            checkVal(ct, neu);
        });
    }


    createIndex(t: _Transaction, expressions: CreateIndexDef): this;
    createIndex(t: _Transaction, expressions: string[], type: 'primary' | 'unique', indexName?: string): this;
    createIndex(t: _Transaction, expressions: string[] | CreateIndexDef, _type?: 'primary' | 'unique', _indexName?: string): this {
        if (this.readonly) {
            throw new PermissionDeniedError(this.name);
        }
        if (Array.isArray(expressions)) {
            const keys: CreateIndexColDef[] = [];
            for (const e of expressions) {
                const parsed = parseSql(e, 'expr');
                const getter = buildValue(this.selection, parsed);
                keys.push({
                    value: getter,
                });
            }
            return this.createIndex(t, {
                columns: keys,
                primary: _type === 'primary',
                notNull: _type === 'primary',
                unique: !!_type,
                indexName: _indexName,
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


        const ihash = indexHash(expressions.columns.map(x => x.value));

        let indexName: string;
        if (expressions.indexName) {
            indexName = expressions.indexName;
            if (this.ownerSchema.getOwnObject(indexName)) {
                if (expressions.ifNotExists) {
                    return this;
                }
                throw new QueryError(`relation "${indexName}" already exists`);
            }
        } else {
            const baseName = indexName = expressions.indexName ?? `${this.name}_${ihash}_idx`;
            let i = 1;
            while (this.ownerSchema.getOwnObject(indexName)) {
                indexName = baseName + (i++);
            }
        }


        const index = new BIndex(t, indexName, expressions.columns, this, ihash, !!expressions.unique, !!expressions.notNull);

        // fill index (might throw if constraint not respected)
        const bin = this.bin(t);
        for (const e of bin.values()) {
            index.add(e, t);
        }

        // =========== reference index ============
        this.indexHandlers.forEach(h => h('create', index));
        // ⚠⚠ This must be done LAST, to avoid throwing an execption if index population failed
        for (const col of index.expressions) {
            for (const used of col.usedColumns) {
                this.getColumnRef(used.id!).usedInIndexes.add(index);
            }
        }
        this.indexByHash.set(ihash, { index, expressions: index.expressions });
        if (expressions.primary) {
            this.hasPrimary = true;
        }
        return this;
    }

    dropIndex(t: _Transaction, uName: string) {
        const u = asIndex(this.ownerSchema.getOwnObject(uName)) as BIndex;
        if (!u || !this.indexByHash.has(u.hash)) {
            throw new QueryError('Cannot drop index that does not belong to this table: ' + uName);
        }
        this.indexHandlers.forEach(h => h('drop', u));
        this.indexByHash.delete(u.hash);
        u.dropFromData(t);
        this.ownerSchema._reg_unregister(u);
    }


    onIndex(sub: IndexHandler): ISubscription {
        this.indexHandlers.add(sub);
        return {
            unsubscribe: () => this.indexHandlers.delete(sub),
        };
    }

    listIndices(): IndexDef[] {
        return [...this.indexByHash.values()]
            .map<IndexDef>(x => ({
                name: x.index.name!,
                expressions: x.expressions.map(x => x.sql!),
            }));
    }

    addForeignKey(cst: TableConstraintForeignKey, t: _Transaction, constraintName?: string) {
        const ftable = this.ownerSchema.getTable(cst.foreignTable) as MemoryTable;
        const cols = cst.localColumns.map(x => this.getColumnRef(x));
        const fcols = cst.foreignColumns.map(x => ftable.getColumnRef(x));
        if (cols.length !== fcols.length) {
            throw new QueryError('Foreign key count mismatch');
        }
        cols.forEach((c, i) => {
            if (fcols[i].expression.type !== c.expression.type) {
                throw new QueryError(`Foreign key column type mismatch`);
            }
        });
        if (cst.onDelete !== 'no action' || cst.onUpdate !== 'no action') {
            throw new NotSupported('Foreign keys with actions not yet supported');
        }

        // check that there is an unique index on this table for the given expressions
        const ihash = indexHash(fcols.map(x => x.expression));
        if (!ftable.indexByHash.get(ihash)?.index?.unique) {
            throw new QueryError(`there is no unique constraint matching given keys for referenced table "${this.name}"`);
        }


        // auto-create indices
        if (this.db.options.autoCreateForeignKeyIndices) {
            this.createIndex(t, {
                ifNotExists: true,
                columns: cols.map<CreateIndexColDef>(x => ({
                    value: x.expression,
                })),
            });
        }


        // when changing the foreign table key, must not be anything in this table that matches
        ftable.onChange(cst.foreignColumns, (old, _, dt) => {
            if (!old) {
                return;
            }
            const vals = fcols.map(x => old[x.expression.id!]);
            if (vals.some(nullIsh)) {
                return;
            }
            // build foreign key equality expression
            const equals = cst.localColumns.map<ExprBinary>((x, i) => ({
                type: 'binary',
                op: '=',
                left: { type: 'ref', name: x, table: this.name },
                // hack, see #fkcheck
                right: {
                    type: 'constant',
                    value: vals[i],
                    dataType: fcols[i].expression.type as any, // hack
                },
            }));
            const expr = equals.slice(1).reduce<Expr>((a, b) => ({
                type: 'binary',
                op: 'AND',
                left: a,
                right: b,
            }), equals[0]);

            // check nothing matches
            for (const _ of this.selection.filter(expr).enumerate(dt)) {
                throw new QueryError(`update or delete on table "${ftable.name}" violates foreign key constraint on table "${this.name}"`);
            }
        });

        // when changing something in this table, then there must be a key match in the foreign table
        const thisSub = this.onChange(cst.localColumns, (_, neu, dt) => {
            if (!neu) {
                return;
            }
            const vals = cols.map(x => (neu as any)[x.expression.id!]);
            if (vals.some(nullIsh)) {
                return;
            }
            // build foreign key equality expression
            const equals = cst.foreignColumns.map<ExprBinary>((x, i) => ({
                type: 'binary',
                op: '=',
                left: { type: 'ref', name: x, table: ftable.name },
                // hack, see #fkcheck
                right: {
                    type: 'constant',
                    value: vals[i],
                    dataType: cols[i].expression.type as any, // hack
                },
            }));
            const expr = equals.slice(1).reduce<Expr>((a, b) => ({
                type: 'binary',
                op: 'AND',
                left: a,
                right: b,
            }), equals[0]);

            // check there is a match
            let yielded = false;
            for (const _ of ftable.selection.filter(expr).enumerate(dt)) {
                yielded = true;
            }
            if (!yielded) {
                throw new QueryError(`insert or update on table "${ftable.name}" violates foreign key constraint on table "${this.name}"`);
            }
        });


        ftable.onDrop(() => {
            thisSub.unsubscribe()
        });
    }

    addConstraint(cst: TableConstraint, t: _Transaction) {
        // todo add constraint name
        switch (cst.type) {
            case 'foreign key':
                return this.addForeignKey(cst, t, cst.constraintName);
            case 'primary key':
                return this.createIndex(t, cst.columns, 'primary', cst.constraintName);
            case 'unique':
                return this.createIndex(t, cst.columns, 'unique', cst.constraintName);
            case 'check':
                return this.addCheck(t, cst.expr, cst.constraintName);
            default:
                throw NotSupported.never(cst, 'constraint type');
        }
    }

    onChange(columns: string[], check: ChangeHandler<T>): ISubscription {
        this.changeHandlers.add(check);
        const reset: (() => void)[] = [];
        for (const c of columns) {
            const hs = this.getColumnRef(c).changeHandlers;
            hs.add(check);
            reset.push(() => hs.delete(check));
        }
        return {
            unsubscribe: () => {
                this.changeHandlers.delete(check);
                reset.forEach(d => d());
            }
        }
    }

    drop(t: _Transaction) {
        t.delete(this.dataId);
        this.drophandlers.forEach(d => d(t));
        for (const i of this.indexByHash.values()) {
            i.index.dropFromData(t);
        }
        // todo should also check foreign keys, cascade, ...
        return this.ownerSchema._reg_unregister(this);
    }

    onDrop(sub: DropHandler): ISubscription {
        this.drophandlers.add(sub);
        return {
            unsubscribe: () => {
                this.drophandlers.delete(sub);
            }
        }
    }
}
