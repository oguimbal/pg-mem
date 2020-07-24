import { TableConstraint, DataTypeDef, Expr } from './parser/syntax/ast';


export interface Schema {
    name: string;
    fields: SchemaField[];
    constraints?: TableConstraint[];
}

export interface SchemaField {
    id: string;
    type: IType;
    primary?: boolean;
    unique?: boolean;
    notNull?: boolean;
    autoIncrement?: boolean;
    default?: Expr;
}

export interface IType {
    /** Data type */
    readonly primary: DataType;
    toString(): string;
}

// todo support all types https://www.postgresql.org/docs/9.5/datatype.html
export enum DataType {
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

export interface IMemoryDb {
    readonly adapters: LibAdapters;
    readonly public: ISchema;
    getSchema(name: string): ISchema;
    createSchema(name: string): ISchema;
    getTable(table: string): IMemoryTable;
    on(event: GlobalEvent, handler: () => any);
    on(event: TableEvent, handler: (table: string) => any);
}

export interface LibAdapters {
    /** Create a PG module that will be equivalent to require('pg') */
    createPg(queryLatency?: number): { Pool: any; Client: any };
    /**  */
    createPgPromise(queryLatency?: number): any;
    /** Hook a not-yet-connected Typeorm connection */
    createTypeormConnection(typeOrmConnection: any, queryLatency?: number);
}

export interface ISchema {
    many(query: string): any[];
    none(query: string): void;
    declareTable(table: Schema): IMemoryTable;
    query(text: string): QueryResult;
}

export interface QueryResult {
    /** Last command that has been executed */
    command: 'UPDATE' | 'INSERT' | 'CREATE' | 'SELECT' | 'ALTER';
    rowCount: number;
    fields: Array<FieldInfo>;
    rows: any[];
}

export interface FieldInfo {
    name: string;
}



export type TableEvent = 'seq-scan';
export type GlobalEvent = 'catastrophic-join-optimization';

export interface IMemoryTable {
    // createIndex(expressions: string[]): this;
    on(event: TableEvent, handler: () => any): void;
}

export interface IndexDef {

}

export class CastError extends Error {
    constructor(from: DataType, to: DataType, inWhat?:string) {
        super(`failed to cast ${from} to ${to}`+ (inWhat ? ' in ' + inWhat : ''));
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
        super('Not supported' + (what ? ': ' + what : ''));
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