import { Schema, IMemoryDb, ISchema, TableEvent, GlobalEvent, TableNotFound, QueryError } from './interfaces';
import { MemoryTable } from './table';
import { _IDb, _ISelection, _ITable, _Transaction, _IQuery } from './interfaces-private';
import { Query } from './query';
import { initialize } from './transforms/transform-base';
import { buildSelection, buildAlias } from './transforms/selection';
import { buildFilter } from './transforms/build-filter';
import { Adapters } from './adapters';
import { Transaction } from './transaction';

export function newDb(): IMemoryDb {
    initialize({
        buildSelection,
        buildAlias,
        buildFilter,
    });
    return new MemoryDb();
}

class MemoryDb implements _IDb {

    private handlers = new Map<TableEvent | GlobalEvent, Set<(...args: any[]) => any>>();
    readonly data = Transaction.root();
    private schemas = new Map<string, _IQuery>();

    readonly adapters = new Adapters(this);
    get public() {
        return this.getSchema(null)
    }

    constructor() {
        this.createSchema('public');
        this.declareSchema('information_schema')
            .informationSchma();
    }

    declareSchema(name: string) {
        if (this.schemas.has(name)) {
            throw new Error('Schema exists: ' + name);
        }
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

    getSchema(db: string): _IQuery {
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