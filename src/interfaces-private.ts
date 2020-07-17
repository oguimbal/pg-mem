import { IMemoryDb, IMemoryTable, DataType, IType, TableEvent } from './interfaces';
import { AST, ColumnRef } from 'node-sql-parser';

export * from './interfaces';

// export type PrimaryKey = string | number;
const ID = Symbol('_id');
export function getId(item: any): string {
    const got = item[ID];
    if (!got) {
        throw new Error('Unexpected: cannot get an ID of something which is not a record');
    }
    return got;
}

export function setId(item: any, id: string) {
    const got = item[ID];
    if (got) {
        throw new Error('Unexpected: Cannto update an ID');
    }
    item[ID] = id;
}

export interface _ISelectionSource<T = any> {
    /** Statistical measure of how many items will be returned by this selection */
    readonly entropy: number;
    enumerate(): Iterable<T>;
    hasItem(value: T): boolean;

    /** Gets the index associated with this value (or returns null) */
    getIndex(forValue: IValue): _IIndex<any>;
}

export interface _ISelection<T = any> extends _ISelectionSource {
    readonly columns: IValue[];
    filter(where: any): _ISelection;
    select(select: any[] | '*'): _ISelection;
    getColumn(column: string): IValue;
}

export interface _IDb extends IMemoryDb {
    raise(table: string, event: TableEvent): void;
}

export interface _ITable<T = any> extends _ISelectionSource, IMemoryTable {
    readonly selection: _ISelection<T>;
    insert(toInsert: T): void;
    createIndex(expressions: string[] | CreateIndexDef): this;
}

export interface CreateIndexDef {
    columns: CreateIndexColDef[];
    indexName?: string;
    unique?: boolean;
    notNull?: boolean;
    primary?: boolean;
}
export interface CreateIndexColDef {
    value: IValue;
    nullsLast?: boolean;
    desc?: boolean
}

export interface _IDb extends IMemoryDb {
    getTable(name: string): _ITable;
}

export interface _IType<TRaw = any> extends IType {
    /** Data type */
    readonly primary: DataType;

    toString(): string;
    equals(a: TRaw, b: TRaw): boolean;
    gt(a: TRaw, b: TRaw): boolean;
    lt(a: TRaw, b: TRaw): boolean;
    canConvert(to: DataType | _IType<TRaw>): boolean;
    convert<T = any>(value: IValue<TRaw>, to: DataType | _IType<T>): IValue<T>;
}

export interface IValue<TRaw = any> {
    readonly type: _IType<TRaw>;

    readonly isConstant: boolean;

    /** Will be set if there is an index on this value */
    readonly index: _IIndex;

    /** Originates from this selection */
    readonly selection: _ISelection;


    /** Column ID, or null */
    readonly id: string;

    /** Hash of this value (used to identify indexed expressions) */
    readonly hash: string;

    /** Clean debug reconsitutition of SQL used to parse this value */
    readonly sql: string;

    get(raw: TRaw): any;
    setId(newId: string): IValue;
    canConvert(to: DataType | _IType): boolean;
    convert<T = any>(to: DataType | _IType<T>): IValue<T>;
}

export type IndexKey = any[];
export interface _IIndex<T = any> {
    /** How many items in this index */
    readonly size: number;
    readonly indexName: string;
    /** Returns a measure of how many items there are per index key */
    readonly entropy: number;
    readonly expressions: IValue[];
    readonly onTable: _ITable<T>;
    /** Check if THIS record exists in this index */
    hasItem(raw: T): boolean;
    /** Check if this key is present in this index */
    hasKey(key: IndexKey[]): boolean;
    /** Add an element to this index */
    add(raw: T): void;

    /** Get values equating the given key */
    eq(rawKey: IndexKey): Iterable<T>;
    /** Get all values that are NOT  equating any of the given keys */
    nin(rawKey: IndexKey[]): Iterable<T>;
    /** Get values NOT equating the given key */
    neq(rawKey: IndexKey): Iterable<T>;
    /** Get greater the given key */
    gt(rawKey: IndexKey): Iterable<T>;
    /** Get lower the given key */
    lt(rawKey: IndexKey): Iterable<T>;
    /** Get greater or equal the given key */
    ge(rawKey: IndexKey): Iterable<T>;
    /** Get lower or equal the given key */
    le(rawKey: IndexKey): Iterable<T>;
}


export type AST2 = AST | CreateTable;
export interface CreateTable {
    type: 'create';
    keyword: string;
    create_definitions: CreateTable_Def[];
    table?: CreateTable_Table[];

    // NOT HANDLED
    as?: any;
    if_not_exists?: any;
    ignore_replace?: any;
    query_expr?: any;
    table_options?: any;
    temporary?: any;
}
interface CreateTable_Table {
    table: string;

    // NOT HANDLED
    as?: any;
    db?: any;
}

interface CreateTable_Def {
    column: ColumnRef;
    definition: CreateTable_ColDef;
    resource: 'column';
    unique_or_primary?: 'primary key' | 'unique';

    // NOT HANDLED
    auto_increment?: any;
    collate?: any;
    column_format?: any;
    comment?: any;
    default_val?: any;
    nullable?: any;
    reference_definition?: any;
    storage?: any;
}

interface CreateTable_ColDef {
    suffix?: any[];
    dataType: string;
    length?: number;
}
