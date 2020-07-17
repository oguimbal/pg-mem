

export interface Schema {
    name: string;
    fields: SchemaField[];
}

export interface SchemaField {
    id: string;
    type: IType;
    primary?: boolean;
    unique?: boolean;
    notNull?: boolean;
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
    json = 'json',
    blob = 'blob',
    timestamp = 'timestamp',
    date = 'date',
    null = 'null',
    bool = 'bool',
}

export interface IMemoryDb {
    /** Declares the existence of a table */
    declareTable(table: Schema): IMemoryTable;
    query: IQuery;
    getTable(table: string): IMemoryTable;
    on(event: TableEvent, handler: (table: string) => any);
}

export interface IQuery {
    many(query: string): any[];
    none(query: string): void;
}


export type TableEvent = 'seq-scan';
export interface IMemoryTable {
    createIndex(expressions: string[]): this;
    on(event: TableEvent, handler: () => any): void;
}

export interface IndexDef {

}

export class CastError extends Error {
    constructor(from: DataType, to: DataType) {
        super(`failed to cast ${from} to ${to}`);
    }
}
export class ColumnNotFound extends Error {
    constructor(columnName: string) {
        super(`column "${columnName}" does not exist`);
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