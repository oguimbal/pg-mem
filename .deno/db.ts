import { Schema, IMemoryDb, ISchema, TableEvent, GlobalEvent, QueryError, IBackup, MemoryDbOptions, ISubscription, LanguageCompiler, nil } from './interfaces.ts';
import { _IDb, _ISelection, _ITable, _Transaction, _ISchema, _FunctionDefinition, GLOBAL_VARS, _IType, _OperatorDefinition, IValue } from './interfaces-private.ts';
import { DbSchema } from './schema/schema.ts';
import { initialize } from './transforms/transform-base.ts';
import { buildSelection } from './transforms/selection.ts';
import { buildAlias } from './transforms/alias.ts';
import { buildFilter } from './transforms/build-filter.ts';
import { Adapters } from './adapters/index.ts';
import { Transaction } from './transaction.ts';
import { buildGroupBy } from './transforms/aggregation.ts';
import { buildLimit } from './transforms/limit.ts';
import { buildUnion } from './transforms/union.ts';
import { buildDistinct } from './transforms/distinct.ts';
import { buildOrderBy } from './transforms/order-by.ts';
import { setupPgCatalog } from './schema/pg-catalog/index.ts';
import { setupInformationSchema } from './schema/information-schema/index.ts';
import { QName, BinaryOperator } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { asSingleQName } from './utils.ts';

export function newDb(opts?: MemoryDbOptions): IMemoryDb {
    initialize({
        buildSelection,
        buildAlias,
        buildFilter,
        buildGroupBy,
        buildLimit,
        buildUnion,
        buildOrderBy,
        buildDistinct,
    });
    // root transaction
    const root = Transaction.root();
    const globals = root.getMap(GLOBAL_VARS)
        .set('server_version', '12.2 (pg-mem)');
    root.set(GLOBAL_VARS, globals);

    // create db
    return new MemoryDb(root, undefined, opts ?? {});
}

class MemoryDb implements _IDb {

    private handlers = new Map<TableEvent | GlobalEvent, Set<(...args: any[]) => any>>();
    private schemas = new Map<string, _ISchema>();

    schemaVersion = 1;

    readonly adapters: Adapters = new Adapters(this);
    private extensions: { [name: string]: (schema: ISchema) => void } = {};
    private languages: { [name: string]: LanguageCompiler } = {};
    readonly searchPath = ['pg_catalog', 'public'];

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

    registerLanguage(languageName: string, compiler: LanguageCompiler): this {
        this.languages[languageName.toLowerCase()] = compiler;
        return this;
    }

    getLanguage(name: string): LanguageCompiler {
        const ret = this.languages[name.toLowerCase()];
        if (!ret) {
            throw new QueryError(`Unkonwn language "${name}". If you plan to use a script language, you must declare it to pg-mem via ".registerLanguage()"`);
        }
        return ret;
    }


    getExtension(name: string): (schema: ISchema) => void {
        const ret = this.extensions[name];
        if (!ret) {
            throw new Error('Extension does not exist: ' + name);
        }
        return ret;
    }


    createSchema(name: string): DbSchema {
        if (this.schemas.has(name)) {
            throw new Error('Schema exists: ' + name);
        }
        this.onSchemaChange();
        const ret = new DbSchema(name, this);
        this.schemas.set(name, ret);
        return ret;
    }

    getTable(name: string): _ITable;
    getTable(name: string, nullIfNotExists?: boolean): _ITable | null {
        return this.public.getTable(name, nullIfNotExists);
    }

    resolveFunction(name: string | QName, types: IValue[]): _FunctionDefinition | nil {
        const asSingle = asSingleQName(name);
        if (asSingle) {
            for (const sp of this.searchPath) {
                const found = this.getSchema(sp).resolveFunction(name, types, true);
                if (found) {
                    return found;
                }
            }
            return null;
        } else {
            const q = name as QName;
            return this.getSchema(q.schema!).resolveFunction(q.name, types, true);
        }
    }

    resolveOperator(name: BinaryOperator, left: IValue, right: IValue): _OperatorDefinition | nil {
        for (const sp of this.searchPath) {
            const found = this.getSchema(sp).resolveOperator(name, left, right, true);
            if (found) {
                return found;
            }
        }
        return null;
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


    getSchema(db?: string | null, nullIfNotFound?: false): _ISchema;
    getSchema(db: string, nullIfNotFound: true): _ISchema | null;
    getSchema(db?: string | null, nullIfNotFound?: boolean): _ISchema | null {
        db = db ?? 'public';
        const got = this.schemas.get(db);
        if (!got) {
            if (nullIfNotFound) {
                return null;
            }
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