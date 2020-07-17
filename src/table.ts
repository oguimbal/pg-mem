import { IMemoryTable, Schema, SchemaField, DataType, QueryError, RecordExists, TableEvent } from './interfaces';
import { _ISelection, IValue, _ITable, setId, getId, CreateIndexDef, CreateIndexColDef, _IDb } from './interfaces-private';
import { buildValue } from './predicate';
import { Parser } from 'node-sql-parser';
import { BIndex } from './btree-index';
import { Selection } from './transforms/selection';

export class MemoryTable<T = any> implements IMemoryTable, _ITable<T> {

    private all = new Map<string, T>();
    private _schema: Schema;
    private indicesByHash = new Map<string, BIndex<T>>();
    private indicesByName = new Map<string, BIndex<T>>();
    private handlers = new Map<TableEvent, Set<() => void>>();

    readonly selection: _ISelection<T>;
    private it = 0;
    private hasPrimary: boolean;

    get entropy() {
        return this.all.size;
    }

    constructor(private owner: _IDb, schema: Schema) {
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
        this.owner.raise(this._schema.name, event);
    }


    enumerate(): Iterable<T> {
        this.raise('seq-scan');
        return this.all.values();
    }

    insert(toInsert: T): void {
        const newId = this._schema.name + '_' + (this.it++);
        setId(toInsert, newId);
        this.indexElt(toInsert);
        this.all.set(newId, toInsert);
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

    createIndex(expressions: string[] | CreateIndexDef, type?: 'primary' | 'unique'): this {
        if (Array.isArray(expressions)) {
            const keys: CreateIndexColDef[] = [];
            for (const e of expressions) {
                const parser = new Parser();
                const parsed = parser.astify('select * from x where ' + e, {
                    database: 'PostgresQL',
                });
                if (!('type' in parsed) || parsed.type !== 'select') {
                    throw new QueryError('Invalid index syntax: ' + e);
                }
                const getter = buildValue(this.selection, parsed.where);
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
        const index = new BIndex(expressions.columns, this, expressions.indexName ?? ihash, expressions.unique, expressions.notNull);
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