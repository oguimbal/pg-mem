

export interface Schema {
    name: string;
    fields: SchemaField[];
}

export interface SchemaField {
    id: string;
    type: IType;
    primary?: boolean;
    notNull?: boolean;
}

export interface IType {
    /** Data type */
    readonly primary: DataType;
    toString(): string;
}

export enum DataType {
    text = 'text',
    array = 'array',
    long = 'long',
    double = 'double',
    jsonb = 'jsonb',
    json = 'json',
    blob = 'blob',
    timestamp = 'timestamp',
    null = 'null',
    bool = 'bool',
}

export interface IMemoryDb {
    /** Declares the existence of a table */
    declareTable(table: Schema): IMemoryTable;
    query: IQuery;
    getTable(table: string): IMemoryTable;
}

export interface IQuery {
    many(query: string): Promise<any[]>;
    none(query: string): Promise<void>;
}

export type TableEvent = 'seq-scan';
export interface IMemoryTable {
    createIndex(expressions: string[]): this;
    on(event: TableEvent, handler: () => any): void;

}

export class CastError extends Error {
    constructor(from: DataType, to: DataType) {
        super(`failed to cast ${from} to ${to}`);
    }
}
export class ColumnNotFound extends Error {
    constructor(columnName: string) {
        super('Column not found: ' + columnName);
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