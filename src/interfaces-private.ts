import { IMemoryDb, IMemoryTable, DataType } from './interfaces';

export * from './interfaces';

// export type PrimaryKey = string | number;

export interface _ISelectionSource<T = any> {
    /** Statistical measure of how many items will be returned by this selection */
    readonly entropy: number;
    enumerate(): Iterable<T>;
    hasItem(value: T): boolean;

    /** Gets the index associated with this value (or returns null) */
    getIndex(forValue: IValue): _IIndex<any>;

    /** Rebuilds a clean statement */
    sql(state?: BuildState): string;
}

export interface _ISelection<T = any> extends _ISelectionSource {
    readonly columns: IValue[];
    filter(where: any): _ISelection;
    select(select: any[] | '*'): _ISelection;
    getColumn(column: string): IValue;
}
export interface BuildState {
    alias: number;
}


export interface _ITable<T = any> extends _ISelectionSource, IMemoryTable {
    readonly selection: _ISelection<T>;
    insert(toInsert: T): void;
}

export interface _IDb extends IMemoryDb {
    getTable(name: string): _ITable;
}

export interface IValue<TRaw = any> {
    readonly isConstant: boolean;

    /** Will be set if there is an index on this value */
    readonly index: _IIndex;

    /** Originates from this selection */
    readonly selection: _ISelection;

    /** Data type */
    readonly type: DataType;

    /** Column ID, or null */
    readonly id: string;

    /** Hash of this value (used to identify indexed expressions) */
    readonly hash: string;

    /** Clean debug reconsitutition of SQL used to parse this value */
    readonly sql: string;

    canConvert(to: DataType): boolean;
    convert(to: DataType): IValue;
    equals(a: TRaw, b: TRaw): boolean;
    gt(a: TRaw, b: TRaw): boolean;
    lt(a: TRaw, b: TRaw): boolean;
    get(raw: TRaw): any;
    setId(newId: string): IValue;
}

export interface _IIndex<T = any> {
    /** How many items in this index */
    readonly size: number;
    /** Returns a measure of how many items there are per index key */
    readonly entropy: number;
    readonly expressions: IValue[];
    readonly onTable: _ITable<T>;
    hasItem(raw: T): boolean;
    add(raw: T): void;

    /** Get values equating the given key */
    eq(rawKey: any[]): Iterable<T>;
    /** Get greater the given key */
    gt(rawKey: any[]): Iterable<T>;
    /** Get lower the given key */
    lt(rawKey: any[]): Iterable<T>;
    /** Get greater or equal the given key */
    ge(rawKey: any[]): Iterable<T>;
    /** Get lower or equal the given key */
    le(rawKey: any[]): Iterable<T>;
}
