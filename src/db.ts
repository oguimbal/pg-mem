import { Schema, IMemoryDb, ISchema, TableEvent, GlobalEvent, TableNotFound, QueryError, IBackup, MemoryDbOptions } from './interfaces';
import { _IDb, _ISelection, _ITable, _Transaction, _ISchema } from './interfaces-private';
import { Query } from './query';
import { initialize } from './transforms/transform-base';
import { buildSelection } from './transforms/selection';
import { buildAlias } from './transforms/alias';
import { buildFilter } from './transforms/build-filter';
import { Adapters } from './adapters';
import { Transaction } from './transaction';
import { buildGroupBy } from './transforms/aggregation';
import { buildLimit } from './transforms/limit';

export function newDb(opts?: MemoryDbOptions): IMemoryDb {
    initialize({
        buildSelection,
        buildAlias,
        buildFilter,
        buildGroupBy,
        buildLimit,
    });
    return new MemoryDb(Transaction.root(), null, opts ?? {});
}

class MemoryDb implements _IDb {

    private handlers = new Map<TableEvent | GlobalEvent, Set<(...args: any[]) => any>>();
    private schemas = new Map<string, _ISchema>();

    schemaVersion = 1;

    readonly adapters = new Adapters(this);
    get public() {
        return this.getSchema(null)
    }

    onSchemaChange() {
        this.schemaVersion++;
        this.raiseGlobal('schema-change');
    }

    constructor(public data: Transaction, schemas?: Map<string, _ISchema>, readonly options: MemoryDbOptions = {}) {
        if (!schemas) {
            this.createSchema('public');
        } else {
            this.schemas = schemas;
        }
        this.declareSchema('information_schema')
            .informationSchma();
    }

    backup(): IBackup {
        return new Backup(this);
    }

    declareSchema(name: string) {
        if (this.schemas.has(name)) {
            throw new Error('Schema exists: ' + name);
        }
        this.onSchemaChange();
        const ret = new Query(name, this);
        this.schemas.set(name, ret);
        return ret;
    }

    createSchema(name: string) {
        return this.declareSchema(name)
            .pgSchema();
    }

    getTable(name: string, nullIfNotExists?: boolean): _ITable {
        return this.public.getTable(name, nullIfNotExists);
    }

    on(event: GlobalEvent | TableEvent, handler: (...args: any[]) => any) {
        let lst = this.handlers.get(event);
        if (!lst) {
            this.handlers.set(event, lst = new Set());
        }
        lst.add(handler);
    }

    raiseTable(table: string, event: TableEvent): void {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h(table);
        }
    }

    raiseGlobal(event: GlobalEvent): void {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h();
        }
    }

    getSchema(db: string): _ISchema {
        db = db ?? 'public';
        const got = this.schemas.get(db);
        if (!got) {
            throw new QueryError('schema not found: ' + db);
        }
        return got;
    }

    listSchemas() {
        return [...this.schemas.values()];
    }

}

class Backup implements IBackup {
    private readonly data: Transaction;
    private readonly schemaVersion: number;
    constructor(private db: MemoryDb) {
        this.data = db.data.clone();
        this.schemaVersion = db.schemaVersion;
    }

    restore() {
        if (this.schemaVersion !== this.db.schemaVersion) {
            throw new Error('You cannot restore this backup: schema has been changed since this backup has been created => prefer .clone() in this kind of cases.');
        }
        this.db.data = this.data;
    }
}