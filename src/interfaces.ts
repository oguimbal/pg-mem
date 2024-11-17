import { BinaryOperator, CreateColumnDef, DataTypeDef, FunctionArgumentMode, NodeLocation, Statement, TableConstraint } from 'pgsql-ast-parser';
import { MigrationParams } from './migrate/migrate-interfaces';


export type nil = undefined | null;


export type Schema = {
    name: string;
    fields: SchemaField[];
    constraints?: TableConstraint[];
}


export interface SchemaField extends Omit<CreateColumnDef, 'dataType' | 'kind' | 'name'> {
    type: IType | DataType;
    name: string;
    serial?: boolean;
}

export interface IType {
    /** Data type */
    readonly primary: DataType;
    readonly name: string;
    toString(): string;

    /** Create an array type of this type */
    asArray(): IType;
}

// todo support all types https://www.postgresql.org/docs/9.5/datatype.html
export enum DataType {

    inet = 'inet',
    record = 'record',
    uuid = 'uuid',
    text = 'text',
    citext = 'citext',
    array = 'array',
    list = 'list',
    bigint = 'bigint',
    float = 'float',
    decimal = 'decimal',
    integer = 'integer',
    jsonb = 'jsonb',
    regtype = 'regtype',
    regclass = 'regclass',
    json = 'json',
    bytea = 'bytea',
    interval = 'interval',
    timestamp = 'timestamp',
    timestamptz = 'timestamptz',
    date = 'date',
    time = 'time',
    timetz = 'timetz',
    null = 'null',
    bool = 'bool',

    point = 'point',
    line = 'line',
    lseg = 'lseg',
    box = 'box',
    path = 'path',
    polygon = 'polygon',
    circle = 'circle',
}

export interface MemoryDbOptions {
    /**
     * If set to true, pg-mem will stop embbeding info about the SQL statement
     * that has failed in exception messages.
     */
    noErrorDiagnostic?: boolean;
    /**
     * If set to true, then the query runner will not check that no AST part
     * has been left behind when parsing the request.
     *
     * ... so setting it to true could lead to unnoticed ignored query parts.
     *
     * (advice: only set it to true as a workaround while an issue on https://github.com/oguimbal/pg-mem is being fixed... )
     */
    noAstCoverageCheck?: boolean;
    /**
     *  If set to true, this will throw an exception if
     * you try to use an unsupported index type
     * (only BTREE is supported at time of writing)
     */
    noIgnoreUnsupportedIndices?: boolean;
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
    getTable<T = any>(table: string): IMemoryTable<T>;
    getTable<T = any>(table: string, nullIfNotFound?: boolean): IMemoryTable<T> | null;

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

    /** Registers a new language, usable in 'DO' blocks, or in 'CREATE FUNCTION' blocks */
    registerLanguage(languageName: string, compiler: LanguageCompiler): this;
}

export type QueryInterceptor = (query: string) => any[] | nil;


export type ArgDef = DataType | IType | ArgDefDetails;

export interface ArgDefDetails {
    /** Argument type */
    type: IType;
    /** Optional argument name */
    name?: string;
    /**
     *  Arguments are 'in' by default, but you can change that.
     */
    mode?: FunctionArgumentMode;
}

export type LanguageCompiler = (options: ToCompile) => CompiledFunction;

export interface ToCompile {
    /** Function being compiled (null for "DO" statements compilations) */
    functionName?: string | nil;
    /** Code to compile */
    code: string;
    /** Schema against which this compilation is performed */
    schema: ISchema;
    /** Expected arguments */
    args: ArgDefDetails[];
    /** Expected return type (if any) */
    returns?: IType | nil;
}

export class AdvancedResult {
    constructor(readonly result: any, outArgs: any[]) {
    }
}

export type CompiledFunction = (...inArguments: any[]) => AdvancedResult | PlainResult;

export type PlainResult = Object | number | Date | null | void;

export interface CompiledFunctionResult {
    /** The function result, if function "returns" something */
    result?: any;
    /** The functions out arguments, as indexed in the `args` passed to your language compiler function */
    outArgs?: any[];
}

export interface IBackup {
    /**
     * Restores data to the state when this backup has been performed.
     * 👉 This operation is O(1).
     * 👉 Schema must not have been changed since then !
     **/
    restore(): void;
}

export interface ISerializedDb {
    data: string;
    schemas: string[];
    options: MemoryDbOptions;
}

export interface LibAdapters {
    /** Create a PG module that will be equivalent to require('pg') */
    createPg(queryLatency?: number): { Pool: any; Client: any };

    /** Create a pg-promise instance bound to this db */
    createPgPromise(queryLatency?: number): any;

    /** Create a slonik pool bound to this db */
    createSlonik(opts?: SlonikAdapterOptions): Promise<any>;

    /** Create a pg-native instance bound to this db */
    createPgNative(queryLatency?: number): any;

    /** Create a Typeorm connection bound to this db
     * @deprecated Use `createTypeormDataSource` instead. See https://github.com/oguimbal/pg-mem/pull/238.
     */
    createTypeormConnection(typeOrmConnection: any, queryLatency?: number): any;

    /** Create a Typeorm data source bound to this db */
    createTypeormDataSource(typeOrmConnection: any, queryLatency?: number): any;

    /** Create a Knex.js instance bound to this db */
    createKnex(queryLatency?: number, knexConfig?: object): any;

    /** Create a Kysely instance bound to this db */
    createKysely(queryLatency?: number, kyselyConfig?: object): any;

    /** Create a mikro-orm instance bound to this db */
    createMikroOrm(mikroOrmOptions: any, queryLatency?: number): Promise<any>

    /** Creates a Postres.js `sql` tag bound to this db */
    createPostgresJsTag(queryLatency?: number): any;

    /** Binds a server to this instance */
    bindServer(opts?: BindServerOptions): Promise<BindServerResult>;
}

export interface BindServerResult {
    postgresConnectionString: string;
    connectionSettings: {
        host: string;
        port: number;
    };
    close(): void;
}

export interface BindServerOptions {
    /** defaults to a random port */
    port?: number;
    /** defaults to 'localhost' */
    host?: string;
}

export interface SlonikAdapterOptions {
    queryLatency?: number;
    /** options you would give to the createPool() function */
    createPoolOptions?: any;
    /** add zod validation interceptor to create pool options, like with https://github.com/gajus/slonik?tab=readme-ov-file#result-parser-interceptor */
    zodValidation?: boolean;
    /** old versions of slonik, this be passed as client configuration  */
    clientConfigurationInput?: any;
}

export type QueryOrAst = string | Statement | Statement[];

export interface IPreparedQuery {
    describe(): QueryDescription;
    bind(args?: any[]): IBoundQuery;
}

export interface QueryDescription {
    parameters: ParameterInfo[];
    result: FieldInfo[];
}

export interface ParameterInfo {
    type: DataType;
    typeId: number
}

export interface IBoundQuery {
    /** Executes all statements */
    executeAll(): QueryResult;
    /**
     * Progressively executes a query that contains multiple statements, yielding results until the end of enumeration (or an exception)
     */
    iterate(): IterableIterator<QueryResult>;
}

export interface ISchema {
    /**
     * Another way to create tables (equivalent to "create table" queries")
     */
    declareTable(table: Schema): IMemoryTable<any>;

    /**
         * Execute a query and return many results
         */
    many(query: QueryOrAst): any[];
    /**
     * Execute a query without results
     */
    none(query: QueryOrAst): void;
    /**
     * Execute a query with a single result
     */
    one(query: QueryOrAst): any;

    /**
     * Execute a query that has no argument, and returns the latest query result
     *   (shortcut for .prepare(cmd).bind().executeAll())
     */
    query(text: QueryOrAst): QueryResult;

    /**
     * Progressively executes a query that has no argument, yielding results until the end of enumeration (or an exception)
     *  (shortcut for .prepare(cmd).bind().iterate())
     */
    queries(text: QueryOrAst): Iterable<QueryResult>;

    /**
     * Prepare a query
     */
    prepare(text: QueryOrAst): IPreparedQuery;

    /**
     * Get a table in this db to inspect it
     */
    getTable(table: string): IMemoryTable<unknown>;
    getTable(table: string, nullIfNotFound?: boolean): IMemoryTable<unknown> | null;

    /**
     * List all tables in this schema
     */
    listTables(): Iterable<IMemoryTable<unknown>>

    /** Register a function */
    registerFunction(fn: FunctionDefinition, orReplace?: boolean): this;


    /** Register a binary operator */
    registerOperator(fn: OperatorDefinition): this;

    /** Register a simple type, which is equivalent to another */
    registerEquivalentType(type: IEquivalentType): IType;

    /** Register a simple type, which is equivalent to another */
    registerEquivalentSizableType(type: IEquivalentType): IType;

    /** Get an existing type */
    getType(name: DataType): IType;

    /**
     * Registers an enum type on this schema
     * @param name Enum name
     * @param values Possible values
     */
    registerEnum(name: string, values: string[]): void;

    /**
     * Database migration, node-sqlite flavor
     * ⚠ Only working when runnin nodejs !
     */
    migrate(config?: MigrationParams): Promise<void>;


    /**
     * Intecept queries.
     * If your interceptor returns an array, then the query will not be executed.
     * The given result will be returned instead.
     */
    interceptQueries(interceptor: QueryInterceptor): ISubscription;
}

export interface FunctionDefinition {
    /** Function name (casing doesnt matter) */
    name: string;

    /** Expected arguments */
    args?: ArgDef[] | nil;

    /** Other arguments type (variadic arguments) */
    argsVariadic?: DataType | IType | nil;

    /** Returned data type */
    returns?: DataType | IType | nil;

    /**
     * If the function is marked as impure, it will not be simplified
     * (ex: "select myFn(1) from myTable" will call myFn() for each row in myTable, even if it does not depend on its result) */
    impure?: boolean;

    /** If true, the function will also be called when passing null arguments */
    allowNullArguments?: boolean;

    /** Actual implementation of the function */
    implementation: CompiledFunction;
}

export interface OperatorDefinition {
    /** Function name (casing doesnt matter) */
    operator: BinaryOperator;

    /** Expected left argument */
    left: DataType | IType;

    /** Expected right argument */
    right: DataType | IType;

    /** True if the operator is commutative (if left & right can be inverted) */
    commutative?: boolean;

    /** Returned data type */
    returns: DataType | IType;

    /**
     * If the function is marked as impure, it will not be simplified
     * (ex: "select myFn(1) from myTable" will call myFn() for each row in myTable, even if it does not depend on its result) */
    impure?: boolean;

    /** If true, the function will also be called when passing null arguments */
    allowNullArguments?: boolean;

    /** Actual implementation of the function */
    implementation: CompiledFunction;
}


export interface QueryResult {
    /** Last command that has been executed */
    command: string;
    rowCount: number;
    fields: Array<FieldInfo>;
    rows: any[];

    /** Location of the last ";" prior to this statement */
    location: NodeLocation;
}

export interface FieldInfo {
    name: string;
    type: DataType;
    typeId: number;
}



export type TableEvent = 'seq-scan';
export type GlobalEvent = 'query' | 'query-failed' | 'catastrophic-join-optimization' | 'schema-change' | 'create-extension';

export interface IMemoryTable<T> {
    readonly name: string;
    readonly primaryIndex: IndexDef | nil;

    /** List columns in this table */
    getColumns(): Iterable<ColumnDef>;

    /** Subscribe to an event on this table */
    on(event: TableEvent, handler: () => any): ISubscription;
    /** List existing indices defined on this table */
    listIndices(): IndexDef[];


    /**
     * Inserts a raw item into this table.
     * ⚠ Neither the record you provided, nor the returned value are the actual item stored. You wont be able to mutate internal state.
     * @returns A copy of the inserted item (with assigned defaults)
     */
    insert(item: Partial<T>): T | null;

    /** Find all items matching a specific template */
    find<keys extends (keyof T)[]>(template?: Partial<T> | nil, columns?: keys): Pick<T, ArrayToUnionOr<keys, keyof T>>[];
}


type ArrayToUnionOr<T, Or> = T extends (infer U)[] ? U : Or;

export interface ColumnDef {
    readonly name: string;
    readonly type: IType;
    readonly nullable: boolean;
}


export interface ISubscription {
    unsubscribe(): void;
}

export interface IndexDef {
    readonly name: string;
    readonly expressions: string[];
}

export class NotSupported extends Error {
    constructor(what?: string) {
        super('🔨 Not supported 🔨 ' + (what ? ': ' + what : ''));
    }

    static never(value: never, msg?: string) {
        return new NotSupported(`${msg ?? ''} ${JSON.stringify(value)}`);
    }
}


interface ErrorData {
    readonly error: string;
    readonly details?: string;
    readonly hint?: string;
    readonly code?: string;
}
export class QueryError extends Error {
    readonly data: ErrorData;
    readonly code: string | undefined;
    constructor(err: string | ErrorData, code?: string) {
        super(typeof err === 'string' ? err : errDataToStr(err));
        if (typeof err === 'string') {
            this.data = { error: err, code };
            this.code = code;
        } else {
            this.data = err;
            this.code = err.code;
        }
    }
}

function errDataToStr(data: ErrorData) {
    const ret = ['ERROR: ' + data.error];
    if (data.details) {
        ret.push('DETAIL: ' + data.details);
    }
    if (data.hint) {
        ret.push('HINT: ' + data.hint)
    }
    return ret.join('\n');
}


export class CastError extends QueryError {
    constructor(from: string | DataType | IType, to: string | DataType | IType, inWhat?: string) {
        super(`cannot cast type ${typeof from === 'string'
            ? from
            : from.name} to ${typeof to === 'string'
                ? to
                : to.name}`
            + (inWhat ? ' in ' + inWhat : ''));
    }
}


export class ColumnNotFound extends QueryError {
    constructor(col: string) {
        super(`column "${col}" does not exist`);
    }
}

export class AmbiguousColumn extends QueryError {
    constructor(col: string) {
        super(`column reference "${col}" is ambiguous`);
    }
}

export class RelationNotFound extends QueryError {
    constructor(tableName: string) {
        super(`relation "${tableName}" does not exist`);
    }
}
export class TypeNotFound extends QueryError {
    constructor(t: string | number | DataTypeDef) {
        super(`type "${typeof t !== 'object' ? t : typeDefToStr(t)}" does not exist`);
    }
}

export class RecordExists extends QueryError {
    constructor() {
        super('Records already exists');
    }
}


export class PermissionDeniedError extends QueryError {
    constructor(what?: string) {
        super(what
            ? `permission denied: "${what}" is a system catalog`
            : 'permission denied');
    }
}


export function typeDefToStr(t: DataTypeDef): string {
    if (t.kind === 'array') {
        return typeDefToStr(t.arrayOf) + '[]';
    }
    let ret = t.name;
    if (t.schema) {
        ret = t.schema + '.' + ret;
    }
    if (t.config?.length) {
        ret = ret + '(' + t.config.join(',') + ')';
    }
    return ret;
}

/** A type definition that is equivalent to another type */
export interface IEquivalentType {
    /** Type name */
    readonly name: string;
    /** Which underlying type is it equivalent to ? */
    readonly equivalentTo: DataType | IType;
    /**
     * Is this value valid ?
     */
    isValid(value: any): boolean;
}
