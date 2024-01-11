import { IMemoryTable, Schema, QueryError, TableEvent, PermissionDeniedError, NotSupported, IndexDef, ISubscription, nil, ColumnDef } from './interfaces.ts';
import { IValue, _ITable, setId, getId, CreateIndexDef, CreateIndexColDef, _Transaction, _ISchema, _Column, _IType, SchemaField, _IIndex, _Explainer, _SelectExplanation, ChangeHandler, Stats, DropHandler, IndexHandler, asIndex, Reg, ChangeOpts, _IConstraint, TruncateHandler, TruncateOpts } from './interfaces-private.ts';
import { buildValue } from './parser/expression-builder.ts';
import { BIndex } from './schema/btree-index.ts';
import { columnEvaluator } from './transforms/selection.ts';
import { nullIsh, deepCloneSimple, Optional, indexHash, findTemplate, colByName } from './utils.ts';
import { Map as ImMap } from 'https://deno.land/x/immutable@4.0.0-rc.12-deno.1/mod.ts';
import { CreateColumnDef, TableConstraintForeignKey, TableConstraint, Expr, Name, ExprRef } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ColRef } from './column.ts';
import { buildAlias, Alias } from './transforms/alias.ts';
import { DataSourceBase } from './transforms/transform-base.ts';
import { ForeignKey } from './constraints/foreign-key.ts';
import { Types } from './datatypes/index.ts';
import { withSelection } from './parser/context.ts';
import { SubscriptionConstraint } from './constraints/subscription.ts';
import { ConstraintWrapper } from './constraints/wrapped.ts';
import { IndexConstraint } from './constraints/index-cst.ts';


type Raw<T> = ImMap<string, T>;


interface ChangeSub<T> {
    before: Set<ChangeHandler<T>>;
    after: Set<ChangeHandler<T>>;
}

interface ChangePlan<T> {
    before(): void
    after(): void;
}

class ColumnManager {
    private _columns?: readonly IValue[];
    readonly map = new Map<string, ColRef>();

    get columns(): readonly IValue[] {
        if (!this._columns) {
            this._columns = Object.freeze(Array.from(this.map.values(), c => c.expression));
        }
        return this._columns!;
    }
    invalidateColumns() {
        this._columns = undefined;
    }

    // Pass-through methods
    get = this.map.get.bind(this.map);
    has = this.map.has.bind(this.map)
    values = this.map.values.bind(this.map);

    set(name: string, colDef: ColRef) {
        this.invalidateColumns();
        return this.map.set(name, colDef);
    }

    delete(name: string) {
        this.invalidateColumns();
        return this.map.delete(name);
    }
}

export class MemoryTable<T = any> extends DataSourceBase<T> implements IMemoryTable, _ITable<T> {
    get isExecutionWithNoResult(): boolean {
        return false;
    }
    private handlers = new Map<TableEvent, Set<() => void>>();
    readonly selection: Alias<T>;
    private _reg?: Reg;
    get reg(): Reg {
        if (!this._reg) {
            throw new QueryError(`relation "${this.name}" does not exist`);
        }
        return this._reg;
    }
    get columns() {
        return this.columnMgr.columns;
    }
    private it = 0;
    private cstGen = 0;
    private hasPrimary: BIndex | null = null;
    private readonly = false;
    hidden = false;
    private dataId = Symbol();
    private serialsId: symbol = Symbol();
    private constraintsByName = new Map<string, _IConstraint>();
    private indexByHashAndName = new Map<string, Map<string, {
        index: BIndex<T>;
        expressions: IValue[];
    }>>();
    readonly columnMgr = new ColumnManager();
    name: string;

    private changeHandlers = new Map<_Column | null, ChangeSub<T>>();
    private truncateHandlers = new Set<TruncateHandler>();
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

    register() {
        // once fields registered,
        //  then register the table
        //  (column registrations need it not to be registered yet)
        this._reg = this.ownerSchema._reg_register(this);
        return this;
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

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue<any> | nil {
        return colByName(this.columnMgr.map, column, nullIfNotFound)
            ?.expression;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            _: 'table',
            table: this.name,
        };
    }

    addColumn(column: SchemaField | CreateColumnDef, t: _Transaction): _Column {
        if ('dataType' in column) {
            const tp: SchemaField = {
                ...column,
                name: column.name.name,
                type: this.ownerSchema.getType(column.dataType),
            };
            delete (tp as any as Optional<CreateColumnDef>).dataType;
            return this.addColumn(tp, t);
        }

        if (this.columnMgr.has(column.name)) {
            throw new QueryError(`Column "${column.name}" already exists`);
        }
        const type = typeof column.type === 'string'
            ? this.ownerSchema.getType(column.type)
            : column.type;
        const cref = new ColRef(this, columnEvaluator(this.selection, column.name, type as _IType), column, column.name);


        // auto increments
        if (column.serial) {
            t.set(this.serialsId, t.getMap(this.serialsId).set(column.name, 0));
        }

        this.columnMgr.set(column.name, cref);

        try {
            if (column.constraints?.length) {
                cref.addConstraints(column.constraints, t);
            }
            const hasDefault = column.constraints?.some(x => x.type === 'default');
            if (!hasDefault) {
                this.remapData(t, x => (x as any)[column.name] = (x as any)[column.name] ?? null);
            }
        } catch (e) {
            this.columnMgr.delete(column.name);
            throw e;
        }

        // once constraints created, reference them. (constraint creation might have thrown)m
        this.db.onSchemaChange();
        this.selection.rebuild();
        return cref;
    }


    getColumnRef(column: string): ColRef;
    getColumnRef(column: string, nullIfNotFound?: boolean): ColRef | nil;
    getColumnRef(column: string, nullIfNotFound?: boolean): ColRef | nil {
        const got = this.columnMgr.get(column);
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

    find(template?: T, columns?: (keyof T)[]): Iterable<T> {
        return findTemplate(this.selection, this.db.data, template, columns);
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

    insert(toInsert: T): T | null {
        const ret = this.doInsert(this.db.data, deepCloneSimple(toInsert));
        if (ret == null) {
            return null
        }
        return deepCloneSimple(ret);
    }

    doInsert(t: _Transaction, toInsert: T, opts?: ChangeOpts): T | null {
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
        for (const c of this.columnMgr.values()) {
            c.setDefaults(toInsert, t);
        }

        // check change handlers (foreign keys)
        const changePlan = this.changePlan(t, null, toInsert, opts);
        changePlan.before();

        // check "on conflict"
        const onConflict = opts?.onConflict;
        if (onConflict) {
            if ('ignore' in onConflict) {
                if (onConflict.ignore === 'all') {
                    for (const map of this.indexByHashAndName.values()) {
                        for (const k of map.values()) {
                            if (!k.index.unique) {
                                continue;
                            }
                            const key = k.index.buildKey(toInsert, t);
                            const found = k.index.eqFirst(key, t);
                            if (found) {
                                // This function returns the inserted row,
                                // but in this case we had a conflict and no row was inserted.
                                // So we return null.
                                return null; // ignore.
                            }
                        }
                    }
                } else {
                    const index = onConflict.ignore as BIndex;
                    const key = index.buildKey(toInsert, t);
                    const found = index.eqFirst(key, t);
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
                    onConflict.update(got, toInsert, t);
                    return this.update(t, got);
                }
            }
        }

        // check constraints
        for (const c of this.columnMgr.values()) {
            c.checkConstraints(toInsert, t);
        }

        // check change handlers (foreign keys)
        changePlan.after();

        // index & check indx contrainsts
        this.indexElt(t, toInsert);
        this.setBin(t, this.bin(t).set(newId, toInsert));
        return toInsert;
    }

    private changePlan(t: _Transaction, old: T | null, neu: T | null, _opts: ChangeOpts | nil): ChangePlan<T> {
        const opts = _opts ?? {};
        let iter: () => IterableIterator<ChangeSub<T>>;
        if (!old || !neu) {
            iter = () => this.changeHandlers.values();
        } else {
            const ret: ChangeSub<T>[] = [];
            const global = this.changeHandlers.get(null);
            if (global) {
                ret.push(global);
            }
            for (const def of this.columnMgr.values()) {
                const h = this.changeHandlers.get(def);
                if (!h) {
                    continue;
                }
                const oldVal = (old as any)[def.expression.id!];
                const neuVal = (neu as any)[def.expression.id!];
                if (def.expression.type.equals(oldVal, neuVal)) {
                    continue;
                }
                ret.push(h);
            }
            iter = ret[Symbol.iterator].bind(ret);
        }
        return {
            before: () => {
                const ran = new Set();
                for (const { before } of iter()) {
                    for (const b of before) {
                        if (!b || ran.has(b)) {
                            continue;
                        }
                        b(old, neu, t, opts);
                        ran.add(b);
                    }
                }
            },
            after: () => {
                const ran = new Set();
                for (const { after } of iter()) {
                    for (const a of after) {
                        if (!a || ran.has(a)) {
                            continue;
                        }
                        a(old, neu, t, opts);
                        ran.add(a);
                    }
                }
            },
        }
    }

    update(t: _Transaction, toUpdate: T): T {
        if (this.readonly) {
            throw new PermissionDeniedError(this.name);
        }
        const bin = this.bin(t);
        const id = getId(toUpdate);
        const exists = bin.get(id) ?? null;

        // set default values
        for (const c of this.columnMgr.values()) {
            c.setDefaults(toUpdate, t);
        }



        // check change handlers (foreign keys)
        const changePlan = this.changePlan(t, exists, toUpdate, null);
        changePlan.before();
        changePlan.after();


        // check constraints
        for (const c of this.columnMgr.values()) {
            c.checkConstraints(toUpdate, t);
        }

        // remove old version from index
        if (exists) {
            for (const map of this.indexByHashAndName.values()) {
                for (const k of map.values()) {
                    k.index.delete(exists, t);
                }
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
        const changePlan = this.changePlan(t, toDelete, null, null);
        changePlan.before();
        changePlan.after();

        // remove from indices
        for (const map of this.indexByHashAndName.values()) {
            for (const k of map.values()) {
                k.index.delete(got, t);
            }
        }
        this.setBin(t, bin.delete(id));

        return got;
    }

    truncate(t: _Transaction, _opts?: TruncateOpts): void {
        const opts: TruncateOpts = _opts ?? {};
        // call truncate handlers
        for (const h of this.truncateHandlers) {
            h(t, opts);
        }
        // truncate indices
        for (const map of this.indexByHashAndName.values()) {
            for (const k of map.values()) {
                k.index.truncate(t);
            }
        }
        this.setBin(t, ImMap());
    }


    private indexElt(t: _Transaction, toInsert: T) {
        for (const map of this.indexByHashAndName.values()) {
            for (const k of map.values()) {
                k.index.add(toInsert, t);
            }
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
        if (this.hasPrimary?.hash === ihash) {
            return this.hasPrimary;
        }
        const map = this.indexByHashAndName.get(ihash);
        const got = map?.values().next().value;
        return got?.index ?? null;
    }

    constraintNameGen(constraintName?: string) {
        return constraintName
            ?? (this.name + '_constraint_' + (++this.cstGen));
    }

    addCheck(_t: _Transaction, check: Expr, constraintName?: string): _IConstraint {
        constraintName = this.constraintNameGen(constraintName);
        this.checkNoConstraint(constraintName);
        const getter = withSelection(this.selection, () => buildValue(check).cast(Types.bool));

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
        const sub = this.onBeforeChange('all', (old, neu, ct) => {
            if (!neu) {
                return;
            }
            checkVal(ct, neu);
        });

        const ret = new SubscriptionConstraint(constraintName, () => sub.unsubscribe());
        return new ConstraintWrapper(this.constraintsByName, ret);
    }


    createIndex(t: _Transaction, expressions: CreateIndexDef): _IConstraint | nil;
    createIndex(t: _Transaction, expressions: Name[], type: 'primary' | 'unique', indexName?: string | nil): _IConstraint;
    createIndex(t: _Transaction, expressions: Name[] | CreateIndexDef, _type?: 'primary' | 'unique', _indexName?: string): _IConstraint | nil {
        if (this.readonly) {
            throw new PermissionDeniedError(this.name);
        }
        if (!_indexName && _type === 'primary') {
            _indexName = `${this.name}_pkey`;
        }
        if (Array.isArray(expressions)) {
            const keys: CreateIndexColDef[] = [];
            for (const e of expressions) {
                const getter = this.selection.getColumn(e.name);
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

        const indexName = this.determineIndexRelName(expressions.indexName, ihash, expressions.ifNotExists, 'idx');
        if (!indexName) {
            return null;
        }
        this.checkNoConstraint(indexName);


        const index = new BIndex(t
            , indexName
            , expressions.columns
            , this
            , ihash
            , !!expressions.unique
            , !!expressions.notNull
            , expressions.predicate);

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
        const indexesByHash = this.indexByHashAndName.get(ihash) || new Map<string, { index: BIndex<T>; expressions: IValue[] }>();
        indexesByHash.set(indexName, { index, expressions: index.expressions });
        this.indexByHashAndName.set(ihash, indexesByHash);
        if (expressions.primary) {
            this.hasPrimary = index;
        }
        const ret = new IndexConstraint(indexName, index, this);
        this.constraintsByName.set(indexName, ret);
        return ret;
    }

    private determineIndexRelName(indexName: string | nil, ihash: string, ifNotExists: boolean | nil, sufix: string): string | nil {
        if (indexName) {
            if (this.ownerSchema.getOwnObject(indexName)) {
                if (ifNotExists) {
                    return null;
                }
                throw new QueryError(`relation "${indexName}" already exists`);
            }
            return indexName;
        } else {
            const baseName = indexName = `${this.name}_${ihash}_${sufix}`;
            let i = 1;
            while (this.ownerSchema.getOwnObject(indexName)) {
                indexName = baseName + (i++);
            }
            return indexName!;
        }
    }

    dropIndex(t: _Transaction, uName: string) {
        const u = asIndex(this.ownerSchema.getOwnObject(uName)) as BIndex;
        if (!u || !this.indexByHashAndName.has(u.hash) || !this.indexByHashAndName.get(u.hash)?.has(uName)) {
            throw new QueryError('Cannot drop index that does not belong to this table: ' + uName);
        }
        this.indexHandlers.forEach(h => h('drop', u));
        this.indexByHashAndName.get(u.hash)?.delete(uName);
        if (this.indexByHashAndName.get(u.hash)?.size == 0) {
            this.indexByHashAndName.delete(u.hash);
        }
        u.dropFromData(t);
        this.ownerSchema._reg_unregister(u);
        this.constraintsByName.delete(uName);
        if (this.hasPrimary === u) {
            this.hasPrimary = null;
        }
        for (const col of u.expressions) {
            for (const used of col.usedColumns) {
                this.getColumnRef(used.id!).usedInIndexes.delete(u);
            }
        }
    }


    onIndex(sub: IndexHandler): ISubscription {
        this.indexHandlers.add(sub);
        return {
            unsubscribe: () => this.indexHandlers.delete(sub),
        };
    }

    listIndices(): IndexDef[] {
        return ([] as IndexDef[]).concat(...
            [...this.indexByHashAndName.values()]
                .map(indexMap => [...indexMap.values()]
                    .map<IndexDef>(x => ({
                        name: x.index.name!,
                        expressions: x.expressions.map(x => x.id!)
                    }))
                )
        );
    }

    get primaryIndex(): IndexDef | null {
        return this.hasPrimary && {
            name: this.hasPrimary.name!,
            expressions: this.hasPrimary.expressions.map(x => x.id!)
        };
    }

    addForeignKey(cst: TableConstraintForeignKey, t: _Transaction): _IConstraint | nil {
        const ihash = indexHash(cst.localColumns.map(x => x.name));
        const constraintName = this.determineIndexRelName(cst.constraintName?.name, ihash, false, 'fk');
        if (!constraintName) {
            return null;
        }
        const ret = new ForeignKey(constraintName)
            .install(t, cst, this);
        return new ConstraintWrapper(this.constraintsByName, ret);
    }

    getConstraint(constraint: string): _IConstraint | nil {
        return this.constraintsByName.get(constraint);
    }

    addConstraint(cst: TableConstraint, t: _Transaction): _IConstraint | nil {
        switch (cst.type) {
            case 'foreign key':
                return this.addForeignKey(cst, t);
            case 'primary key':
                return this.createIndex(t, cst.columns, 'primary', cst.constraintName?.name);
            case 'unique':
                return this.createIndex(t, cst.columns, 'unique', cst.constraintName?.name);
            case 'check':
                return this.addCheck(t, cst.expr, cst.constraintName?.name);
            default:
                throw NotSupported.never(cst, 'constraint type');
        }
    }

    private checkNoConstraint(name: string) {
        if (this.constraintsByName.has(name)) {
            throw new QueryError(`relation "${name}" already exists`, '42P07');
        }
    }

    onBeforeChange(columns: 'all' | (string | _Column)[], check: ChangeHandler<T>): ISubscription {
        return this._subChange('before', columns, check);
    }
    onCheckChange(columns: string[], check: ChangeHandler<T>): ISubscription {
        return this._subChange('before', columns, check);
    }

    *getColumns(): Iterable<ColumnDef> {
        for (const c of this.columns) {
            yield {
                name: c.id!,
                type: c.type,
                nullable: !this.columnMgr.get(c.id!)!.notNull,
            };
        }
    }

    private _subChange(key: keyof ChangeSub<T>, columns: 'all' | (string | _Column)[], check: ChangeHandler<T>): ISubscription {
        const unsubs: (() => void)[] = [];

        const add = (ref: _Column | ColRef | null) => {
            let ch = this.changeHandlers.get(ref);
            if (!ch) {
                this.changeHandlers.set(ref, ch = {
                    after: new Set(),
                    before: new Set(),
                });
            }
            ch[key].add(check);
            unsubs.push(() => ch![key].delete(check));
        }
        if (columns === 'all') {
            add(null);
        } else {
            for (const c of columns) {
                const ref = typeof c === 'string'
                    ? this.getColumnRef(c)
                    : c;
                add(ref);
            }
        }
        return {
            unsubscribe: () => {
                for (const u of unsubs) {
                    u();
                }
            }
        }
    }


    drop(t: _Transaction, cascade: boolean) {
        this.drophandlers.forEach(d => d(t, cascade));
        t.delete(this.dataId);
        for (const map of this.indexByHashAndName.values()) {
            for (const i of map.values()) {
                i.index.dropFromData(t);
            }
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

    onTruncate(sub: TruncateHandler): ISubscription {
        this.truncateHandlers.add(sub);
        return {
            unsubscribe: () => {
                this.truncateHandlers.delete(sub);
            }
        }

    }
}
