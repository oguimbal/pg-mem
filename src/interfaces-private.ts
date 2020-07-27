import { IMemoryDb, IMemoryTable, DataType, IType, TableEvent, GlobalEvent, ISchema, SchemaField } from './interfaces';
import { Expr, SelectedColumn, SelectStatement, CreateColumnDef, AlterColumn, DataTypeDef, ConstraintDef, TableRef } from './parser/syntax/ast';
import { Map as ImMap, Record, List, Set as ImSet } from 'immutable';

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

export interface _ISchema extends ISchema {

    readonly name: string;
    readonly db: _IDb;
    buildSelect(p: SelectStatement): _ISelection;
    explainSelect(sql: string): _SelectExplanation;
    explainLastSelect(): _SelectExplanation;
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
    getSet<T>(identity: symbol): ImSet<T>;
}

export interface _ISelection<T = any> {
    readonly schema: _ISchema;
    /** Statistical measure of how many items will be returned by this selection */
    entropy(t: _Transaction): number;
    enumerate(t: _Transaction): Iterable<T>;

    /** Returns true if the given value is present in this */
    hasItem(value: T, t: _Transaction): boolean;

    /** Gets the index associated with this value (or returns null) */
    getIndex(forValue: IValue): _IIndex<T>;
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
    _: 'join';
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
    _: 'map';
    select?: {
        what: _ExprExplanation;
        as: string;
    }[];
    of: _SelectExplanation;
} | {
    /** A table */
    id: number;
    _: 'table';
    table: string;
} | {
    /** An AND filter */
    id: number;
    _: 'and',
    enumerate: _SelectExplanation;
    andCheck: _SelectExplanation[];
} | {
    /** A raw array definition */
    id: number;
    _: 'constantSet';
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
    _: 'eq' | 'ineq' | 'neq' | 'inside' | 'outside';
    entropy: number;
    /** The index that will be used to check equality */
    on: _IndexExplanation;
} | {
    /** An empty set */
    id: number;
    _: 'empty';
} | {
    /** An union set */
    id: number;
    _: 'union',
    union: _SelectExplanation[];
} | {
    /** A seq-scan filter of another set */
    id: number;
    _: 'seqFilter';
    filtered: _SelectExplanation;
}

export type _IndexExplanation = {
    /** BTree index on expression */
    _: 'btree';
    onTable: string;
    btree: string[];
} | {
    _: 'indexMap';
    of: _IndexExplanation;
} | {
    _: 'indexRestriction';
    /** This index will receive a lookup for each item of "for" collection */
    lookup: _IndexExplanation;
    /** Enumerated collection */
    for: _SelectExplanation;
} | {
    _: 'joinIndex';
};

export type _ExprExplanation = {
    constant: true;
} | {
    /** ID of the origin of this selection */
    on: number;
    col: string;
}

export interface _IDb extends IMemoryDb {
    readonly public: _ISchema;
    readonly data: _Transaction;
    getSchema(db: string): _ISchema;
    raiseTable(table: string, event: TableEvent): void;
    raiseGlobal(event: GlobalEvent): void;
    listSchemas(): _ISchema[];
    onSchemaChange(): void;
    getTable(name: string, nullIfNotExists?: boolean): _ITable;
}

export interface _ITable<T = any> extends IMemoryTable {

    readonly hidden: boolean;
    readonly schema: _ISchema;
    readonly name: string;
    readonly selection: _ISelection<T>;
    readonly columnDefs: _Column[];
    insert(t: _Transaction, toInsert: T): T;
    update(t: _Transaction, toUpdate: T): T;
    createIndex(t: _Transaction, expressions: string[] | CreateIndexDef): this;
    setReadonly(): this;
    /** Create a column */
    addColumn(column: SchemaField | CreateColumnDef, t: _Transaction): _Column;
    /** Get a column to modify it */
    getColumnRef(column: string, nullIfNotFound?: boolean): _Column;
    rename(to: string): this;
    addConstraint(constraint: ConstraintDef, t: _Transaction);
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
    constantConverter<TTarget>(_to: DataType | _IType<TTarget>): ((val: TRaw) => TTarget);
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
export interface IndexExpression {
    readonly hash: string;
    readonly type: _IType;
}
export interface _IIndex<T = any> {
    readonly expressions: IndexExpression[];

    /** Returns a measure of how many items will be returned by this op */
    entropy(t: IndexOp): number;

    /** Get values equating the given key */
    eqFirst(rawKey: IndexKey, t: _Transaction): T;

    enumerate(op: IndexOp): Iterable<T>;

    explain(e: _Explainer): _IndexExplanation;
}

export type IndexOp = {
    type: 'eq' | 'neq' | 'gt' | 'lt' | 'ge' | 'le';
    key: IndexKey;
    t: _Transaction;
} | {
    type: 'inside' | 'outside'
    lo: IndexKey;
    hi: IndexKey;
    t: _Transaction;
} | {
    type: 'nin';
    keys: IndexKey[];
    t: _Transaction;
}

export interface TableRecordDef<T> {
    hasPrimary?: boolean;
    readonly?: boolean;
    hidden?: boolean;
    name?: string;
    dataId?: symbol;

    serials: ImMap<string, number>;
    it: number;
    indexByHash: ImMap<string, _IIndex<T>>;
    indexByName: ImMap<string, _IIndex<T>>;
    columnDefs: List<string>;
    columnsByName: ImMap<string, CR<T>>;
}

export interface TableColumnRecordDef<T> {
    default: IValue;
    notNull: boolean;
    usedInIndexes: ImSet<_IIndex>;
    type: _IType;
    name: string;
}

export type TR<T> = Record<TableRecordDef<T>>;
export type CR<T> = Record<TableColumnRecordDef<T>>;
export const EmtpyTable = Record<TableRecordDef<any>>({
    serials: ImMap(),
    it: 0,
    indexByHash: ImMap(),
    indexByName: ImMap(),
    columnDefs: List(),
    columnsByName: ImMap(),
});

export const NewColumn = Record<TableColumnRecordDef<any>>({
    default: null,
    notNull: false,
    usedInIndexes: ImSet(),
    type: null,
    name: null,
});