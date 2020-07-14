import { IMemoryTable, Schema, SchemaField, DataType, QueryError, RecordExists, TableEvent } from './interfaces';
import { _ISelection, IValue, _ITable } from './interfaces-private';
import { buildValue } from './predicate';
import { Parser } from 'node-sql-parser';
import { BIndex } from './btree-index';
import { Selection } from './filters/selection';


export class MemoryTable<T = any> implements IMemoryTable, _ITable<T> {

    private all = new Set<T>();
    private _schema: Schema;
    private indices = new Map<string, BIndex<T>>();
    private handlers = new Map<TableEvent, Set<() => void>>();

    readonly selection: _ISelection<T>;

    get entropy() {
        return this.all.size;
    }

    constructor(schema: Schema) {
        this._schema = schema;
        // this.primary = raw => primaries.map(x => raw[x.id]).join('|');
        this.selection = new Selection(this, this._schema);

        const primaries = schema.fields
            .filter(x => x.primary)
            .map(x => x.id);
        if (primaries.length) {
            this.createIndex(primaries);
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
    }


    enumerate(): Iterable<T> {
        this.raise('seq-scan');
        return this.all;
    }

    insert(toInsert: T): void {
        if (this.all.has(toInsert)) {
            throw new RecordExists();
        }
        for (const k of this.indices.values()) {
            k.add(toInsert);
        }
        this.all.add(toInsert);
    }

    hasItem(item: T) {
        return this.all.has(item);
    }

    getIndex(forValue: IValue) {
        if (forValue.selection !== this.selection) {
            return null;
        }
        const got = this.indices.get(forValue.hash);
        return got ?? null;
    }

    sql() {
        return this._schema.name;
    }

    createIndex(expressions: string[]) {
        if (!expressions.length) {
            throw new QueryError('Empty index');
        }
        const keys: IValue[] = [];
        for (const e of expressions) {
            const parser = new Parser();
            const parsed = parser.astify('select * from x where ' + e, {
                database: 'PostgresQL',
            });
            if (!('type' in parsed) || parsed.type !== 'select') {
                throw new QueryError('Invalid index syntax: ' + e);
            }
            const getter = buildValue(this.selection, parsed.where);
            keys.push(getter);
        }

        const final = keys.map(x => x.hash).sort().join('|');
        if (this.indices.has(final)) {
            throw new QueryError('Index already exists');
        }
        const index = new BIndex(keys, this);
        this.indices.set(final, index);

        for (const e of this.all) {
            index.add(e);
        }
    }
}