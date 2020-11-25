import { Schema, IMemoryDb, ISchema, TableEvent, GlobalEvent, TableNotFound, QueryError, IBackup, MemoryDbOptions, ISubscription } from './interfaces';
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
import { buildOrderBy } from './transforms/order-by';
import { setupPgCatalog } from './schema/pg-catalog';
import { setupInformationSchema } from './schema/information-schema';

export function newDb(opts?: MemoryDbOptions): IMemoryDb {
    initialize({
        buildSelection,
        buildAlias,
        buildFilter,
        buildGroupBy,
        buildLimit,
        buildOrderBy,
    });
    return new MemoryDb(Transaction.root(), undefined, opts ?? {});
}

class MemoryDb implements _IDb {

    private handlers = new Map<TableEvent | GlobalEvent, Set<(...args: any[]) => any>>();
    private schemas = new Map<string, _ISchema>();

    schemaVersion = 1;

    readonly adapters: Adapters = new Adapters(this);
    private extensions: { [name: string]: (schema: ISchema) => void } = {};
    private searchPath = ['pg_catalog', 'public'];

    get public() {
        return this.getSchema(null)
    }

    onSchemaChange() {
        this.schemaVersion++;
        this.raiseGlobal('schema-change', this);
    }

    constructor(public data: Transaction, schemas?: Map<string, _ISchema>, readonly options: MemoryDbOptions = {}) {
        if (!schemas) {
            this.createSchema('public');
        } else {
            this.schemas = schemas;
        }
        setupPgCatalog(this);
        setupInformationSchema(this);
    }

    backup(): IBackup {
        return new Backup(this);
    }

    registerExtension(name: string, install: (schema: ISchema) => void): this {
        this.extensions[name] = install;
        return this;
    }

    getExtension(name: string): (schema: ISchema) => void {
        const ret = this.extensions[name];
        if (!ret) {
            throw new Error('Extension does not exist: ' + name);
        }
        return ret;
    }


    createSchema(name: string): Query {
        if (this.schemas.has(name)) {
            throw new Error('Schema exists: ' + name);
        }
        this.onSchemaChange();
        const ret = new Query(name, this);
        this.schemas.set(name, ret);
        return ret;
    }

    getTable(name: string): _ITable;
    getTable(name: string, nullIfNotExists?: boolean): _ITable | null {
        for (const sp of this.searchPath) {
            const tbl = this.getSchema(sp).getTable(name, true, true);
            if (tbl) {
                return tbl;
            }
        }
        if (nullIfNotExists) {
            return null;
        }
        throw new TableNotFound(name);
    }

    on(event: GlobalEvent | TableEvent, handler: (...args: any[]) => any): ISubscription {
        let lst = this.handlers.get(event);
        if (!lst) {
            this.handlers.set(event, lst = new Set());
        }
        lst.add(handler);
        return {
            unsubscribe: () => lst?.delete(handler),
        };
    }

    raiseTable(table: string, event: TableEvent): void {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h(table);
        }
    }

    raiseGlobal(event: GlobalEvent, ...data: any[]): void {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h(...data);
        }
    }

    getSchema(db?: string | null): _ISchema {
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
        this.db.data = this.data.clone();
    }
}