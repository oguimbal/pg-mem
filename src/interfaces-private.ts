import { IMemoryDb, IMemoryTable, DataType, IType, TableEvent, GlobalEvent, ISchema } from './interfaces';
import { Expr, SelectedColumn, SelectStatement, CreateColumnDef, AlterColumn, DataTypeDef, ConstraintDef, TableRef } from './parser/syntax/ast';
import type { Map as ImMap } from 'immutable';

export * from './interfaces';

// export type PrimaryKey = string | number;
const ID = Symbol('_id');
export function getId(item: any): string {
    if (!item) {
        return '';
    }
    const got = item[ID];
    if (!got) {
        throw new Error('Unexpected: cannot get an ID of something which is not a record');
    }
    return got;
}

export function setId<T = any>(item: T, id: string): T {
    const got = item[ID];
    if (got === id) {
        return item;
    }
    if (got) {
        throw new Error('Unexpected: Cannot update an ID');
    }
    item[ID] = id;
    return item;
}

export interface _IQuery extends ISchema {

    readonly name: string;
    readonly db: _IDb;
    buildSelect(p: SelectStatement): _ISelection;
    explainSelect(sql: string): _SelectExplanation;
    getTable(table: string, nullIfNotFound?: boolean): _ITable;
    tablesCount(t: _Transaction): number;
    listTables(t: _Transaction): Iterable<_ITable>;
    _doRenTab(db: string, to: string);
}


export interface _Transaction {
    readonly isChild: boolean;
    /** Create a new transaction within this transaction */
    fork(): _Transaction;
    /** Commit this transaction (returns the parent transaction) */
    commit(): _Transaction;
    /** Commits this transaction and all underlying transactions */
    fullCommit(): _Transaction;
    rollback(): _Transaction;
    set<T>(identity: symbol, data: T): T;
    get<T>(identity: symbol): T;
    getMap<T extends ImMap<any, any>>(identity: symbol): T;
}

export interface _ISelection<T = any> {
    readonly schema: _IQuery;
    /** Statistical measure of how many items will be returned by this selection */
    entropy(t: _Transaction): number;
    enumerate(t: _Transaction): Iterable<T>;

    /** Returns true if the given value is present in this */
    hasItem(value: T, t: _Transaction): boolean;

    /** Gets the index associated with this value (or returns null) */
    getIndex(forValue: IValue): _IIndex<any>;
    readonly columns: ReadonlyArray<IValue>;
    filter(where: Expr): _ISelection;
    select(select: SelectedColumn[]): _ISelection;
    getColumn(column: string, nullIfNotFound?: boolean): IValue;
    setAlias(alias?: string): _ISelection;
    subquery(data: _ISelection<any>, op: SelectStatement): _ISelection;
    explain(e: _Explainer): _SelectExplanation;
}
export interface _Explainer {
    readonly transaction: _Transaction;
    idFor(sel: _ISelection): number;
}

export type _SelectExplanation = {
    /** A jointure */
    id: number;
    type: 'join';
    join: _SelectExplanation;
    with: _SelectExplanation;
    inner: boolean;
    on: {
        /** 'with' will have to be scanned with this expression */
        seqScan: _ExprExplanation;
    } | {
        /** the 'with' table has this index that can be used */
        index: _IndexExplanation;
        /** It will be matched with this expression (computable from the 'join' table) */
        matches: _ExprExplanation;
    }
} | {
    /** A selection transformation */
    id: number;
    type:'map';
    select?: {
        what: _ExprExplanation;
        as: string;
    }[];
    of: _SelectExplanation;
} | {
    /** A table */
    id: number;
    type: 'table';
    table: string;
} | {
    /** An AND filter */
    id: number;
    type: 'and',
    enumerate: _SelectExplanation;
    andCheck: _SelectExplanation[];
} | {
    /** A raw array definition */
    id: number;
    type: 'constantSet';
    rawArrayLen: number;
} | {
    /** One of the following operators on an index:
     * - (NOT) IN
     * - (NOT) LIKE
     * - (NOT) BETWEEN
     * - < > <= >= = !=
     *
     * (against constants) */
    id: number;
    type: 'eq' | 'ineq' | 'neq' | 'inside' | 'outside';
    /** The index that will be used to check equality */
    on: _IndexExplanation;
} | {
    /** An empty set */
    id: number;
    type: 'empty';
} | {
    /** An union set */
    id: number;
    type: 'union',
    union: _SelectExplanation[];
} | {
    /** A seq-scan filter of another set */
    id: number;
    type: 'seqFilter';
    filter: _SelectExplanation;
}

export type _IndexExplanation = {
    /** BTree index on expression */
    onTable: string;
    btree: string[];
};

export type _ExprExplanation = {
    constant: true;
} | {
    /** ID of the origin of this selection */
    on: number;
    col: string;
}

export interface _IDb extends IMemoryDb {
    readonly public: _IQuery;
    readonly data: _Transaction;
    getSchema(db: string): _IQuery;
    raiseTable(table: string, event: TableEvent): void;
    raiseGlobal(event: GlobalEvent): void;
    listSchemas(): _IQuery[];
}

export interface _ITable<T = any> extends IMemoryTable {

    readonly hidden: boolean;
    readonly schema: _IQuery;
    readonly name: string;
    readonly selection: _ISelection<T>;
    readonly columnDefs: _Column[];
    insert(t: _Transaction, toInsert: T): T;
    update(t: _Transaction, toUpdate: T): T;
    createIndex(t: _Transaction, expressions: string[] | CreateIndexDef): this;
    setReadonly(): this;
    /** Create a column */
    addColumn(column: CreateColumnDefTyped | CreateColumnDef, t: _Transaction): _Column;
    /** Get a column to modify it */
    getColumnRef(column: string, nullIfNotFound?: boolean): _Column;
    rename(to: string): this;
    addConstraint(constraint: ConstraintDef, t: _Transaction);
}

export interface CreateColumnDefTyped extends Omit<CreateColumnDef, 'dataType'> {
    type: _IType;
    serial?: boolean;
}

export interface _Column {
    readonly default: IValue;
    readonly expression: IValue;
    readonly usedInIndexes: ReadonlySet<_IIndex>;
    alter(alter: AlterColumn, t: _Transaction): this;
    rename(to: string, t: _Transaction): this;
    drop(t: _Transaction): void;

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
    getTable(name: string, nullIfNotExists?: boolean): _ITable;
}

export interface _IType<TRaw = any> extends IType {
    /** Data type */
    readonly primary: DataType;
    readonly regTypeName: string;

    toString(): string;
    equals(a: TRaw, b: TRaw): boolean;
    gt(a: TRaw, b: TRaw): boolean;
    ge(a: TRaw, b: TRaw): boolean;
    lt(a: TRaw, b: TRaw): boolean;
    le(a: TRaw, b: TRaw): boolean;
    canConvertImplicit(to: DataType | _IType<TRaw>): boolean;
    canConvert(to: DataType | _IType<TRaw>): boolean;
    convert<T = any>(value: IValue<TRaw>, to: DataType | _IType<T>): IValue<T>;
}

export interface IValue<TRaw = any> {
    /** Columns used in this expression (if any) */
    readonly usedColumns: ReadonlySet<IValue>;

    readonly type: _IType<TRaw>;

    /** is 'any()' call ? */
    readonly isAny: boolean;

    /** Is a constant... i.e. not dependent on columns. ex: (2+2) or NOW() */
    readonly isConstant: boolean;

    /** Is REAL constant (i.e. 2+2, not varying expressions like NOW()) */
    readonly isConstantReal: boolean;

    /** Is a literal constant ? (constant not defined as an operation) */
    readonly isConstantLiteral: boolean;

    /** Will be set if there is an index on this value */
    readonly index: _IIndex;

    /** Originates from this selection */
    readonly origin: _ISelection;


    /** Column ID, or null */
    readonly id: string;

    /** Hash of this value (used to identify indexed expressions) */
    readonly hash: string;

    /** Clean debug reconsitutition of SQL used to parse this value */
    readonly sql: string;

    /** Get value if is a constant */
    get(): any;
    /** Get value if is NOT a constant */
    get(raw: TRaw, t: _Transaction): any;

    setId(newId: string): IValue;
    canConvert(to: DataType | _IType): boolean;
    convert<T = any>(to: DataType | _IType<T>): IValue<T>;

    /**
     * Creates a copy of this column that can
     **/
    setWrapper<TNewRaw>(newOrigin: _ISelection, unwrap: (val: TNewRaw) => TRaw): IValue<TRaw>;

    explain(e: _Explainer): _ExprExplanation;
}

export type IndexKey = any[];
export interface _IIndex<T = any> {
    readonly hash: string;
    readonly indexName: string;
    readonly expressions: IValue[];
    readonly onTable: _ITable<T>;


    /** How many items in this index */
    size(t: _Transaction): number;

    /** Returns a measure of how many items there are per index key */
    entropy(t: _Transaction): number;

    /** Check if THIS record exists in this index */
    hasItem(raw: T, t: _Transaction): boolean;
    /** Check if this key is present in this index */
    hasKey(key: IndexKey[], t: _Transaction): boolean;
    /** Add an element to this index */
    add(raw: T, t: _Transaction): void;

    /** Get values equating the given key */
    eqFirst(rawKey: IndexKey, t: _Transaction): T;
    eq(rawKey: IndexKey, t: _Transaction): Iterable<T>;
    /** Get all values that are NOT  equating any of the given keys */
    nin(rawKey: IndexKey[], t: _Transaction): Iterable<T>;
    /** Get values NOT equating the given key */
    neq(rawKey: IndexKey, t: _Transaction): Iterable<T>;
    /** Get greater the given key */
    gt(rawKey: IndexKey, t: _Transaction): Iterable<T>;
    /** Get lower the given key */
    lt(rawKey: IndexKey, t: _Transaction): Iterable<T>;
    /** Get greater or equal the given key */
    ge(rawKey: IndexKey, t: _Transaction): Iterable<T>;
    /** Get lower or equal the given key */
    le(rawKey: IndexKey, t: _Transaction): Iterable<T>;

    explain(e: _Explainer): _IndexExplanation;
}
