import { IMemoryDb, IMemoryTable, DataType, IType, TableEvent, GlobalEvent, ISchema, SchemaField, MemoryDbOptions, nil, FunctionDefinition, Schema, QueryError, ISubscription, RelationNotFound } from './interfaces.ts';
import { Expr, SelectedColumn, SelectStatement, CreateColumnDef, AlterColumn, LimitStatement, OrderByStatement, TableConstraint, AlterSequenceChange, CreateSequenceOptions, AlterSequenceSetOptions, QName } from 'https://deno.land/x/pgsql_ast_parser@1.3.5/mod.ts';
import { Map as ImMap, Record, List, Set as ImSet } from 'https://deno.land/x/immutable@4.0.0-rc.12-deno.1/mod.ts';

export * from './interfaces.ts';


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
    const got = (item as any)[ID];
    if (got === id) {
        return item;
    }
    if (got) {
        throw new Error('Unexpected: Cannot update an ID');
    }
    (item as any)[ID] = id;
    return item;
}

export type RegClass = string | number;
export type RegType = string | number;

export interface _ISchema extends ISchema {
    readonly name: string;
    readonly db: _IDb;
    buildSelect(p: SelectStatement): _ISelection;
    explainSelect(sql: string): _SelectExplanation;
    explainLastSelect(): _SelectExplanation | undefined;
    getTable(table: string): _ITable;
    getTable(table: string, nullIfNotFound?: boolean): _ITable;
    tablesCount(t: _Transaction): number;
    listTables(t: _Transaction): Iterable<_ITable>;
    declareTable(table: Schema, noSchemaChange?: boolean): _ITable;
    /** Get functions matching this arrity */
    getFunctions(name: string, arrity: number, forceOwn?: boolean): Iterable<_FunctionDefinition>;

    getObject(p: QName): _IRelation;
    getObject(p: QName, opts?: QueryObjOpts): _IRelation | null;

    getOwnObject(name: string): _IRelation | null;


    getObjectByRegClassId(reg: number): _IRelation;
    getObjectByRegClassId(reg: number, opts?: QueryObjOpts): _IRelation | null;

    getOwnObjectByRegClassId(reg: number): _IRelation | null;

    getObjectByRegOrName(reg: RegClass): _IRelation;
    getObjectByRegOrName(reg: RegClass, opts?: QueryObjOpts): _IRelation | null;

    setReadonly(): void;


    _reg_register(rel: _IRelation): Reg;
    _reg_unregister(rel: _IRelation): void;
    _reg_rename(rel: _IRelation, oldName: string, newName: string): void;
}

export interface QueryObjOpts {
    /** Returns null instead of throwing error if not found */
    nullIfNotFound?: boolean;
    /** Will only search in the current schema, or in the targeted schema (not in search path) */
    skipSearch?: boolean;
}

export interface _FunctionDefinition {
    args: _IType[];
    argsVariadic?: _IType;
    returns: _IType;
    impure?: boolean;
    implementation: (...args: any[]) => any;
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
    delete(identity: symbol): void;
    set<T>(identity: symbol, data: T): T;
    get<T>(identity: symbol): T;
    getMap<T extends ImMap<any, any>>(identity: symbol): T;
    getSet<T>(identity: symbol): ImSet<T>;
}

export interface Stats {
    /** Returns this selection size, or null if it cannot be computed without iteration */
    count: number;
}

export interface _ISelection<T = any> {
    readonly debugId?: string;

    readonly ownerSchema: _ISchema;
    readonly db: _IDb;
    /** Statistical measure of how many items will be returned by this selection */
    entropy(t: _Transaction): number;
    enumerate(t: _Transaction): Iterable<T>;

    /** Returns true if the given value is present in this */
    hasItem(value: T, t: _Transaction): boolean;

    stats(t: _Transaction): Stats | null;

    /** Gets the index associated with this value (or returns null) */
    getIndex(...forValue: IValue[]): _IIndex<T> | nil;
    readonly columns: ReadonlyArray<IValue>;
    filter(where: Expr | nil): _ISelection;
    limit(limit: LimitStatement): _ISelection;
    orderBy(orderBy: OrderByStatement[] | nil): _ISelection<any>;
    groupBy(grouping: Expr[] | nil, select: SelectedColumn[]): _ISelection;
    select(select: SelectedColumn[]): _ISelection;
    getColumn(column: string): IValue;
    getColumn(column: string, nullIfNotFound?: boolean): IValue | nil;
    setAlias(alias?: string): _ISelection;
    subquery(data: _ISelection<any>, op: SelectStatement): _ISelection;
    isOriginOf(a: IValue<any>): boolean;
    explain(e: _Explainer): _SelectExplanation;
}
export interface _Explainer {
    readonly transaction: _Transaction;
    idFor(sel: _ISelection): string | number;
}

export type _SelectExplanation = {
    /** A jointure */
    id: string | number;
    _: 'join';
    /**  The restrictive table (the one which MUST have a matched elemnt) */
    restrictive: _SelectExplanation;
    /** The joined table */
    joined: _SelectExplanation;
    inner: boolean;
    on: {
        /** 'with' will have to be scanned with this expression */
        seqScan: _ExprExplanation;
    } | {
        /** Which seq id will be iterated (could be either 'join' or 'with' when there is an inner join) */
        iterate: string | number;
        /** Which iteration side has been chosen (always 'restrictive' for non inner joins) */
        iterateSide: 'joined' | 'restrictive';
        /** the index table on the other table that can be used to lookup corresponding item(s) */
        joinIndex: _IndexExplanation;
        /** It will be matched with this expression (computable from the other table) */
        matches: _ExprExplanation;
        /** True if there is a predicate filter that is also applied (happens when there are 'ANDs' in join condition) */
        filtered?: boolean;
    }
} | {
    /** A selection transformation */
    id: string | number;
    _: 'map';
    select?: {
        what: _ExprExplanation;
        as: string;
    }[];
    of: _SelectExplanation;
} | {
    id: string | number;
    _: 'orderBy';
    of: _SelectExplanation;
} | {
    /** A selection transformation */
    id: string | number;
    _: 'limit';
    take?: number;
    skip?: number;
    on: _SelectExplanation;
} | {
    /** A table */
    _: 'table';
    table: string;
} | {
    /** An AND filter */
    id: string | number;
    _: 'and',
    enumerate: _SelectExplanation;
    andCheck: _SelectExplanation[];
} | {
    /** A raw array definition */
    id: string | number;
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
    id: string | number;
    _: 'eq' | 'ineq' | 'neq' | 'inside' | 'outside';
    entropy: number;
    /** The index that will be used to check equality */
    on: _IndexExplanation;
} | {
    /** An empty set */
    id: string | number;
    _: 'empty';
} | {
    /** An union set */
    id: string | number;
    _: 'union',
    union: _SelectExplanation[];
} | {
    /** A seq-scan filter of another set */
    id: string | number;
    _: 'seqFilter';
    filtered: _SelectExplanation;
} | {
    id: string | number;
    _: 'aggregate';
    aggregator: {
        /** aggregation will iterate the whole lot */
        seqScan: _ExprExplanation;
    } | {
        /** aggregation uses an index items which already contains required aggregations */
        index: _IndexExplanation
    } | {
        /** aggregation is trivial (select count(*) from table) */
        trivial: _ISelection;
    };
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
    /** Uses an index of a column propagated by a join */
    _: 'indexOnJoin';
    /** The in propagated column that is used */
    index: _IndexExplanation;
    /** How elements from the other table will be joined */
    strategy: _IndexExplanation | 'catastrophic';
};

export type _ExprExplanation = {
    constant: true;
} | {
    /** ID of the origin of this selection */
    on: string | number;
    col: string;
}

export interface _IDb extends IMemoryDb {
    readonly options: MemoryDbOptions;
    readonly public: _ISchema;
    readonly data: _Transaction;
    readonly searchPath: ReadonlyArray<string>;

    createSchema(db: string): _ISchema;
    getSchema(db?: string | null): _ISchema;
    raiseTable(table: string, event: TableEvent): void;
    raiseGlobal(event: GlobalEvent, ...args: any[]): void;
    listSchemas(): _ISchema[];
    onSchemaChange(): void;
    getTable(name: string, nullIfNotExists?: boolean): _ITable;
    getExtension(name: string): (schema: ISchema) => void;
    /** Get functions matching this arrity */
    getFunctions(name: string, arrity: number): Iterable<_FunctionDefinition>;
}
export type OnConflictHandler = { ignore: 'all' | _IIndex } | {
    onIndex: _IIndex;
    update: (item: any, excluded: any) => void;
}

export type DropHandler = (t: _Transaction) => void;
export type IndexHandler = (act: 'create' | 'drop', idx: _INamedIndex) => void;

interface _RelationBase {
    readonly name: string;
    readonly reg: Reg;
}

export interface Reg {
    readonly typeId: number;
    readonly classId: number;
}

export interface _ITable<T = any> extends IMemoryTable, _RelationBase {
    readonly type: 'table';
    readonly hidden: boolean;
    readonly db: _IDb;
    readonly ownerSchema: _ISchema;
    readonly selection: _ISelection<T>;
    readonly columnDefs: _Column[];
    insert(t: _Transaction, toInsert: T, onConflict?: OnConflictHandler): T;
    setHidden(): this;
    setReadonly(): this;
    delete(t: _Transaction, toDelete: T): void;
    update(t: _Transaction, toUpdate: T): T;
    createIndex(t: _Transaction, expressions: string[] | CreateIndexDef): this;
    setReadonly(): this;
    /** Create a column */
    addColumn(column: SchemaField | CreateColumnDef, t: _Transaction): _Column;
    /** Get a column to modify it */
    getColumnRef(column: string): _Column;
    getColumnRef(column: string, nullIfNotFound?: boolean): _Column | nil;
    rename(to: string): this;
    addConstraint(constraint: TableConstraint, t: _Transaction, constraintName?: string): void;
    getIndex(...forValues: IValue[]): _IIndex | nil;
    dropIndex(t: _Transaction, name: string): void;
    drop(t: _Transaction): void;
    /** Will be executed when one of the given columns is affected (update/delete) */
    onChange(columns: string[], check: ChangeHandler<T>): ISubscription;
    onDrop(sub: DropHandler): ISubscription;
    onIndex(sub: IndexHandler): ISubscription;
}

export type ChangeHandler<T> = (old: T | null, neu: T | null, t: _Transaction) => void;

export interface _Column {
    readonly default: IValue | nil;
    readonly expression: IValue;
    readonly usedInIndexes: ReadonlySet<_IIndex>;
    alter(alter: AlterColumn, t: _Transaction): this;
    rename(to: string, t: _Transaction): this;
    drop(t: _Transaction): void;
    onDrop(sub: DropHandler): ISubscription;
}

export interface CreateIndexDef {
    ifNotExists?: boolean;
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
    readonly regTypeName: string | null;

    toString(): string;
    equals(a: TRaw, b: TRaw): boolean | null;
    gt(a: TRaw, b: TRaw): boolean | null;
    ge(a: TRaw, b: TRaw): boolean | null;
    lt(a: TRaw, b: TRaw): boolean | null;
    le(a: TRaw, b: TRaw): boolean | null;
    canConvertImplicit(to: DataType | _IType<TRaw>): boolean | nil;
    canConvert(to: DataType | _IType<TRaw>): boolean | nil;
    convert<T = any>(value: IValue<TRaw>, to: DataType | _IType<T>): IValue<T>;
    constantConverter<TTarget>(_to: DataType | _IType<TTarget>): ((val: TRaw) => TTarget);
    prefer(type: _IType<any>): _IType | nil;
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
    readonly index: _IIndex | nil;

    /** Originates from this selection */
    readonly origin: _ISelection | nil;


    /** Column ID, or null */
    readonly id: string | nil;

    /** Hash of this value (used to identify indexed expressions) */
    readonly hash: string;

    /** Clean debug reconsitutition of SQL used to parse this value */
    readonly sql: string | nil;

    /** Get value if is a constant */
    get(): any;
    /** Get value if is NOT a constant */
    get(raw: TRaw, t?: _Transaction | nil): any;

    setId(newId: string): IValue;
    canConvert(to: DataType | _IType): boolean;
    convert<T = any>(to: DataType | _IType<T>): IValue<T>;

    /**
     * Creates a copy of this column that can
     **/
    setWrapper<TNewRaw>(newOrigin: _ISelection, unwrap: (val: TNewRaw) => TRaw): IValue<TRaw>;
    setOrigin(origin: _ISelection): IValue<TRaw>;
    clone(): IValue<any>;

    explain(e: _Explainer): _ExprExplanation;
}

export type IndexKey = any[];
export interface IndexExpression {
    readonly hash: string;
    readonly type: _IType;
}

export interface _INamedIndex<T = any> extends _IIndex<T>, _RelationBase {
    readonly type: 'index';
    readonly onTable: _ITable<T>;
}
export interface _IIndex<T = any> {
    readonly unique?: boolean;
    readonly expressions: IndexExpression[];

    /** Returns a measure of how many items will be returned by this op */
    entropy(t: IndexOp): number;

    /** Returns this selection stats, or null if it cannot be computed without iteration */
    stats(t: _Transaction, key?: IndexKey): Stats | null;

    /** Get values equating the given key */
    eqFirst(rawKey: IndexKey, t: _Transaction): T | null;

    enumerate(op: IndexOp): Iterable<T>;

    explain(e: _Explainer): _IndexExplanation;

    iterateKeys(t: _Transaction): Iterable<IndexKey> | null;
}

export type IndexOp = {
    type: 'eq' | 'neq' | 'gt' | 'lt' | 'ge' | 'le';
    key: IndexKey;
    t: _Transaction;
    matchNull?: boolean;
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
    default: null as any,
    notNull: false,
    usedInIndexes: ImSet(),
    type: null as any,
    name: null as any,
});

export type _IRelation = _ITable | _ISequence | _INamedIndex;

export function asIndex(o: _IRelation): _INamedIndex;
export function asIndex(o: _IRelation | null): _INamedIndex | null;
export function asIndex(o: _IRelation | null) {
    if (!o) {
        return null;
    }
    if (o.type === 'index') {
        return o;
    }
    throw new QueryError(`"${o.name}" is not an index`);
}
export function asSeq(o: _IRelation): _ISequence;
export function asSeq(o: _IRelation | null): _ISequence | null;
export function asSeq(o: _IRelation | null) {
    if (!o) {
        return null;
    }
    if (o.type === 'sequence') {
        return o;
    }
    throw new QueryError(`"${o.name}" is not a sequence`);
}
export function asTable(o: _IRelation): _ITable;
export function asTable(o: _IRelation | null): _ITable | null;
export function asTable(o: _IRelation | null, nullIfNotType?: boolean): _ITable | null;
export function asTable(o: _IRelation | null, nullIfNotType?: boolean) {
    if (!o) {
        return null;
    }
    if (o.type === 'table') {
        return o;
    }
    if (nullIfNotType) {
        return null;
    }
    throw new QueryError(`"${o.name}" is not a table`);
}

export interface _ISequence extends _RelationBase {

    readonly type: 'sequence';
    alter(t: _Transaction, opts: CreateSequenceOptions | AlterSequenceChange): this;
    nextValue(t: _Transaction): number;
    setValue(t: _Transaction, value: number): void;
    currentValue(t: _Transaction): number;
    drop(t: _Transaction): void;
}
