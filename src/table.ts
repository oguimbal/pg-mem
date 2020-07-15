import { IMemoryTable, Schema, SchemaField, DataType, QueryError, RecordExists, TableEvent } from './interfaces';
import { _ISelection, IValue, _ITable, setId, getId } from './interfaces-private';
import { buildValue } from './predicate';
import { Parser } from 'node-sql-parser';
import { BIndex } from './btree-index';
import { Selection } from './filters/selection';

export class MemoryTable<T = any> implements IMemoryTable, _ITable<T> {

    private all = new Map<string, T>();
    private _schema: Schema;
    private indices = new Map<string, BIndex<T>>();
    private handlers = new Map<TableEvent, Set<() => void>>();

    readonly selection: _ISelection<T>;
    private it = 0;

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
        return this.all.values();
    }

    insert(toInsert: T): void {
        const newId = this._schema.name + '_' + (this.it++);
        setId(toInsert, newId);
        for (const k of this.indices.values()) {
            k.add(toInsert);
        }
        this.all.set(newId, toInsert);
    }

    hasItem(item: T) {
        const id = getId(item);
        return this.all.has(id);
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

    createIndex(expressions: string[]): this {
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

        const index = new BIndex(keys, this);

        // create the query index
        this.addIndex(index, keys);
        // create the null index
        // this.addIndex(index, keys.map(x => Values.isNull(x)), true);


        for (const e of this.all.values()) {
            index.add(e);
        }
        return this;
    }

    private addIndex(index: BIndex<T>, keys: IValue<any>[], allowOverwrite = false) {
        const final = keys.map(x => x.hash).sort().join('|');
        if (!allowOverwrite && this.indices.has(final)) {
            throw new QueryError('Index already exists');
        }
        this.indices.set(final, index);
    }
}