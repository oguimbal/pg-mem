import { IMemoryTable, Schema, SchemaField, DataType, QueryError, RecordExists, TableEvent, ReadOnlyError, NotSupported } from './interfaces';
import { _ISelection, IValue, _ITable, setId, getId, CreateIndexDef, CreateIndexColDef, _IDb } from './interfaces-private';
import { buildValue } from './predicate';
import { BIndex } from './btree-index';
import { Selection } from './transforms/selection';
import { parse } from './parser/parser';
import { nullIsh } from './utils';

export class MemoryTable<T = any> implements IMemoryTable, _ITable<T> {

    private all = new Map<string, T>();
    private _schema: Schema;
    private indicesByHash = new Map<string, BIndex<T>>();
    private indicesByName = new Map<string, BIndex<T>>();
    private handlers = new Map<TableEvent, Set<() => void>>();

    readonly selection: _ISelection<T>;
    private it = 0;
    private hasPrimary: boolean;
    private readonly: boolean;
    private serials = new Map<string, number>();
    hidden: boolean;
    private notNulls: Set<string>;

    get entropy() {
        return this.all.size;
    }

    get name() {
        return this._schema.name;
    }

    constructor(readonly db: _IDb, schema: Schema) {
        this._schema = schema;
        // this.primary = raw => primaries.map(x => raw[x.id]).join('|');
        this.selection = new Selection(this, {
            schema: this._schema,
        });

        const primaries = schema.fields
            .filter(x => x.primary)
            .map(x => x.id);
        if (primaries.length) {
            this.createIndex(primaries, 'primary');
        }
        for (const u of schema.fields.filter(x => x.unique)) {
            this.createIndex([u.id], 'unique');
        }
        for (const s of schema.fields.filter(x => x.autoIncrement)) {
            this.serials.set(s.id, 0);
        }

        this.notNulls = new Set(schema.fields
            .filter(x => x.notNull)
            .map(x => x.id));

        for (const c of schema.constraints ?? []) {
            switch (c.type) {
                case 'primary key':
                    if (primaries.length) {
                        throw new QueryError('Dupplicate primary key declaration');
                    }
                    this.createIndex(c.columns, 'primary', c.constraintName);
                    break;
                case 'unique':
                    this.createIndex(c.columns, 'unique', c.constraintName);
                    break;
                default:
                    throw NotSupported.never(c, 'constraint type');
            }
        }
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
        this.db.raiseTable(this._schema.name, event);
    }

    setReadonly() {
        this.readonly = true;
        return this;
    }
    setHidden() {
        this.hidden = true;
        return this;
    }


    enumerate(): Iterable<T> {
        this.raise('seq-scan');
        return this.all.values();
    }

    insert(toInsert: T): T {
        if (this.readonly) {
            throw new ReadOnlyError(this._schema.name);
        }
        const newId = this._schema.name + '_' + (this.it++);
        setId(toInsert, newId);
        // serial (auto increments) columns
        for (const [k, v] of this.serials.entries()) {
            if (!nullIsh(toInsert[k])) {
                continue;
            }
            toInsert[k] = v + 1;
            this.serials.set(k, v + 1);
        }
        this.indexElt(toInsert);
        for (const c of this.notNulls) {
            if (nullIsh(toInsert[c])) {
                throw new QueryError(`null value in column "${c}" violates not-null constraint`);
            }
        }
        this.all.set(newId, toInsert);
        return toInsert;
    }

    private indexElt(toInsert: T) {
        try {
            for (const k of this.indicesByHash.values()) {
                k.add(toInsert);
            }
        } catch (e) {
            // rollback those which have already been added
            for (const k of this.indicesByHash.values()) {
                k.delete(toInsert);
            }
            throw e;
        }
    }

    hasItem(item: T) {
        const id = getId(item);
        return this.all.has(id);
    }

    getIndex(forValue: IValue) {
        if (forValue.origin !== this.selection) {
            return null;
        }
        const got = this.indicesByHash.get(forValue.hash);
        return got ?? null;
    }

    createIndex(expressions: string[] | CreateIndexDef, type?: 'primary' | 'unique', indexName?: string): this {
        if (this.readonly) {
            throw new ReadOnlyError(this._schema.name);
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
            return this.createIndex({
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
            throw new QueryError('Table ' + this._schema.name + ' already has a primary key');
        }
        if (expressions.primary) {
            expressions.notNull = true;
            expressions.unique = true;
        }


        const ihash = expressions.columns.map(x => x.value.hash).sort().join('|');
        const index = new BIndex(expressions.columns, this, indexName ?? expressions.indexName ?? ihash, expressions.unique, expressions.notNull);
        if (this.indicesByHash.has(ihash) || this.indicesByName.has(index.indexName)) {
            throw new QueryError('Index already exists');
        }

        // fill index (might throw if constraint not respected)
        for (const e of this.all.values()) {
            this.indexElt(e);
        }

        // reference index
        this.indicesByHash.set(ihash, index);
        this.indicesByName.set(index.indexName, index);
        if (expressions.primary) {
            this.hasPrimary = true;
        }
        return this;
    }
}