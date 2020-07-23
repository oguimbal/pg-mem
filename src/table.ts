import { IMemoryTable, Schema, SchemaField, DataType, QueryError, RecordExists, TableEvent, ReadOnlyError, NotSupported } from './interfaces';
import { _ISelection, IValue, _ITable, setId, getId, CreateIndexDef, CreateIndexColDef, _IDb, _Transaction, _IQuery } from './interfaces-private';
import { buildValue } from './predicate';
import { BIndex } from './btree-index';
import { Selection } from './transforms/selection';
import { parse } from './parser/parser';
import { nullIsh } from './utils';
import { Map as ImMap } from 'immutable';

type Raw<T> = ImMap<string, T>;
export class MemoryTable<T = any> implements IMemoryTable, _ITable<T> {

    private handlers = new Map<TableEvent, Set<() => void>>();

    readonly selection: _ISelection<T>;
    private it = 0;
    private hasPrimary: boolean;
    private readonly: boolean;
    private serials = new Map<string, number>();
    hidden: boolean;
    private notNulls: Set<string>;
    private dataId = Symbol();
    private indexByHash = new Map<string, BIndex<T>>();
    private indexByName = new Map<string, BIndex<T>>();

    entropy(t: _Transaction) {
        return this.bin(t).size;
    }

    get name() {
        return this._schema.name;
    }

    constructor(readonly schema: _IQuery, t: _Transaction, private _schema: Schema) {
        // this.primary = raw => primaries.map(x => raw[x.id]).join('|');
        this.selection = new Selection(this, {
            schema: this._schema,
        });

        const primaries = _schema.fields
            .filter(x => x.primary)
            .map(x => x.id);
        if (primaries.length) {
            this.createIndex(t, primaries, 'primary');
        }
        for (const u of _schema.fields.filter(x => x.unique)) {
            this.createIndex(t, [u.id], 'unique');
        }
        for (const s of _schema.fields.filter(x => x.autoIncrement)) {
            this.serials.set(s.id, 0);
        }

        this.notNulls = new Set(_schema.fields
            .filter(x => x.notNull)
            .map(x => x.id));

        for (const c of _schema.constraints ?? []) {
            switch (c.type) {
                case 'primary key':
                    if (primaries.length) {
                        throw new QueryError('Dupplicate primary key declaration');
                    }
                    this.createIndex(t,c.columns, 'primary', c.constraintName);
                    break;
                case 'unique':
                    this.createIndex(t, c.columns, 'unique', c.constraintName);
                    break;
                default:
                    throw NotSupported.never(c, 'constraint type');
            }
        }
    }

    private bin(t: _Transaction) {
        return t.getMap<Raw<T>>(this.dataId);
    }

    private setBin(t: _Transaction, val: Raw<T>) {
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
        this.schema.db.raiseTable(this._schema.name, event);
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

    insert(t: _Transaction, toInsert: T, shouldHaveId?: boolean): T {
        if (this.readonly) {
            throw new ReadOnlyError(this._schema.name);
        }

        // get ID of this item
        let newId: string;
        if (shouldHaveId) {
            newId = getId(toInsert);
            if (!newId) {
                throw new Error('Unexpeced update error');
            }
        } else {
            newId = this._schema.name + '_' + (this.it++);
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

        // index & check contrainsts
        this.indexElt(t, toInsert);
        for (const c of this.notNulls) {
            if (nullIsh(toInsert[c])) {
                throw new QueryError(`null value in column "${c}" violates not-null constraint`);
            }
        }
        this.setBin(t, this.bin(t).set(newId, toInsert));
        return toInsert;
    }

    update(t: _Transaction, toUpdate: T): T {
        if (this.readonly) {
            throw new ReadOnlyError(this._schema.name);
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
            throw new QueryError('Table ' + this._schema.name + ' already has a primary key');
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