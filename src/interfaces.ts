import { TableConstraint, CreateColumnDef, StatementLocation } from 'pgsql-ast-parser';

export type nil = undefined | null;

export type Schema = {
    name: string;
    fields: SchemaField[];
    constraints?: TableConstraint[];
}


export interface SchemaField extends Omit<CreateColumnDef, 'dataType'> {
    type: IType;
    serial?: boolean;
}

export interface IType {
    /** Data type */
    readonly primary: DataType;
    toString(): string;
}

// todo support all types https://www.postgresql.org/docs/9.5/datatype.html
export enum DataType {
    uuid = 'uuid',
    text = 'text',
    array = 'array',
    long = 'long',
    float = 'float',
    decimal = 'decimal',
    int = 'int',
    jsonb = 'jsonb',
    regtype = 'regtype',
    json = 'json',
    blob = 'blob',
    timestamp = 'timestamp',
    date = 'date',
    null = 'null',
    bool = 'bool',
}

export interface MemoryDbOptions {
    /**
     * When set to true, this will auto create an index on foreign table when adding a foreign key.
     * 👉 Recommanded when using Typeorm .synchronize(), which creates foreign keys but not indices !
     **/
    readonly autoCreateForeignKeyIndices?: boolean;
}

export interface IMemoryDb {
    /**
     * Adapters to create wrappers of this db compatible with known libraries
     */
    readonly adapters: LibAdapters;
    /**
     * The default 'public' schema
     */
    readonly public: ISchema;
    /**
     * Get an existing schema
     */
    getSchema(db?: string | null): ISchema;
    /**
     * Create a schema in this database
     */
    createSchema(name: string): ISchema;
    /**
     * Get a table to inspect it (in the public schema... this is a shortcut for db.public.getTable())
     */
    getTable(table: string): IMemoryTable;
    getTable(table: string, nullIfNotFound?: boolean): IMemoryTable | null;

    /** Subscribe to a global event */
    on(event: 'query', handler: (query: string) => any): ISubscription;
    on(event: GlobalEvent, handler: () => any): ISubscription;
    on(event: GlobalEvent, handler: () => any): ISubscription;
    /** Subscribe to an event on all tables */
    on(event: TableEvent, handler: (table: string) => any): ISubscription;

    /**
     * Creates a restore point.
     * 👉 This operation is O(1) (instantaneous, even with millions of records).
     * */
    backup(): IBackup;

    /**
     * Registers an extension (that can be installed using the 'create extension' statement)
     * @param name Extension name
     * @param install How to install this extension on a given schema
     */
    registerExtension(name: string, install: (schema: ISchema) => void): this;
}

export interface IBackup {
    /**
     * Restores data to the state when this backup has been performed.
     * 👉 This operation is O(1).
     * 👉 Schema must not have been changed since then !
     **/
    restore(): void;
}

export interface LibAdapters {
    /** Create a PG module that will be equivalent to require('pg') */
    createPg(queryLatency?: number): { Pool: any; Client: any };

    /** Create a pg-promise instance bound to this db */
    createPgPromise(queryLatency?: number): any;

    /** Create a slonik pool bound to this db */
    createSlonik(queryLatency?: number): any;

    /** Create a pg-native instance bound to this db */
    createPgNative(queryLatency?: number): any;

    /** Create a Typeorm connection bound to this db */
    createTypeormConnection(typeOrmConnection: any, queryLatency?: number): any;
}

export interface ISchema {
    /**
     * Execute a query and return many results
     */
    many(query: string): any[];
    /**
     * Execute a query without results
     */
    none(query: string): void;
    /**
     * Execute a query with a single result
     */
    one(query: string): any;
    /**
     * Another way to create tables (equivalent to "create table" queries")
     */
    declareTable(table: Schema): IMemoryTable;
    /**
     * Execute a query
     */
    query(text: string): QueryResult;


    /**
     * Progressively executes a query, yielding results until the end of enumeration (or an exception)
     */
    queries(text: string): Iterable<QueryResult>;

    /**
     * Get a table to inspect it
     */
    getTable(table: string): IMemoryTable;
    getTable(table: string, nullIfNotFound?: boolean): IMemoryTable | null;

    /** Register a function */
    registerFunction(fn: FunctionDefinition): this;
}

export interface FunctionDefinition {
    /** Function name (casing doesnt matter) */
    name: string;

    /** Expected arguments */
    args?: (DataType | IType)[];

    /** Other arguments type (variadic arguments) */
    argsVariadic?: DataType | IType;

    /** Returned data type */
    returns: DataType | IType;

    /**
     * If the function is marked as impure, it will not be simplified
     * (ex: "select myFn(1) from myTable" will call myFn() for each row in myTable, even if it does not depend on its result) */
    impure?: boolean;

    /** Actual implementation of the function */
    implementation: (...args: any[]) => any;
}

export interface QueryResult {
    /** Last command that has been executed */
    command: string;
    rowCount: number;
    fields: Array<FieldInfo>;
    rows: any[];

    /** Ignored (because of an "if not exists" or equivalent) */
    ignored?: boolean;
    /** Location of the last ";" prior to this statement */
    location: StatementLocation;
}

export interface FieldInfo {
    name: string;
}



export type TableEvent = 'seq-scan';
export type GlobalEvent = 'query' | 'query-failed' | 'catastrophic-join-optimization' | 'schema-change' | 'create-extension';

export interface IMemoryTable {
    readonly name: string;
    /** Subscribe to an event on this table */
    on(event: TableEvent, handler: () => any): ISubscription;
    /** List existing indices defined on this table */
    listIndices(): IndexDef[];
}

export interface ISubscription {
    unsubscribe(): void;
}

export interface IndexDef {
    name: string;
    expressions: string[];
}

export class CastError extends Error {
    constructor(from: DataType, to: DataType, inWhat?: string) {
        super(`failed to cast ${from} to ${to}` + (inWhat ? ' in ' + inWhat : ''));
    }
}
export class ColumnNotFound extends Error {
    constructor(columnName: string) {
        super(`column "${columnName}" does not exist`);
    }
}

export class AmbiguousColumn extends Error {
    constructor(columnName: string) {
        super(`column "${columnName}" is ambiguous`);
    }
}

export class TableNotFound extends Error {
    constructor(tableName: string) {
        super('Table not found: ' + tableName);
    }
}

export class QueryError extends Error {
}


export class RecordExists extends Error {
    constructor() {
        super('Records already exists');
    }
}

export class NotSupported extends Error {
    constructor(what?: string) {
        super('🔨 Not supported 🔨 ' + (what ? ': ' + what : '') + `

👉 You can file an issue at https://github.com/oguimbal/pg-mem along with a way to reproduce this issue (if you can), and  the stacktrace:
`);
    }

    static never(value: never, msg?: string) {
        return new NotSupported(`${msg ?? ''} ${JSON.stringify(value)}`);
    }
}
export class ReadOnlyError extends Error {
    constructor(what?: string) {
        super('You cannot modify ' + (what ? ': ' + what : 'this'));
    }
}