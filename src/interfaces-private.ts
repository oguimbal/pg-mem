import { IMemoryDb, IMemoryTable, DataType, IType, TableEvent, GlobalEvent, ISchema, SchemaField, MemoryDbOptions, nil, Schema, QueryError, ISubscription, LanguageCompiler, ArgDefDetails, QueryResult } from './interfaces';
import { Expr, SelectedColumn, SelectStatement, CreateColumnDef, AlterColumn, LimitStatement, OrderByStatement, TableConstraint, AlterSequenceChange, CreateSequenceOptions, QName, DataTypeDef, ExprRef, Name, BinaryOperator, ValuesStatement, CreateExtensionStatement, DropFunctionStatement, ExprCall } from 'pgsql-ast-parser';
import { Map as ImMap, Record, Set as ImSet } from 'immutable';
import { CustomEnumType } from "./datatypes/t-custom-enum";

export * from './interfaces';


export const GLOBAL_VARS = Symbol('_global_vars');

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
export type TypeQuery = DataTypeDef | DataType | number | _IType;
export interface _ISchema extends ISchema {
    readonly name: string;
    readonly db: _IDb;
    readonly dualTable: _ITable;
    /** If the given name refers to another schema, then get it. Else, get this */
    getThisOrSiblingFor(name: QName): _ISchema;
    executeCreateExtension(p: CreateExtensionStatement): void;
    dropFunction(fn: DropFunctionStatement): void;
    explainSelect(sql: string): _SelectExplanation;
    explainLastSelect(): _SelectExplanation | undefined;
    getTable(table: string): _ITable;
    getTable(table: string, nullIfNotFound?: boolean): _ITable;
    tablesCount(t: _Transaction): number;
    listTables(t?: _Transaction): Iterable<_ITable>;
    declareTable(table: Schema, noSchemaChange?: boolean): _ITable;
    createSequence(t: _Transaction, opts: CreateSequenceOptions | nil, name: QName | nil): _ISequence;
    /** Get functions matching this overload */
    resolveFunction(name: string | QName, args: IValue[], forceOwn?: boolean): _FunctionDefinition | nil;
    /** Get an exact function def from its signature (do not use that to resolve overload) */
    getFunction(name: string, args: _IType[]): _FunctionDefinition | nil;
    /** Get operator matching this overload */
    resolveOperator(name: BinaryOperator, left: IValue, right: IValue, forceOwn?: boolean): _OperatorDefinition | nil;

    getObject(p: QName): _IRelation;
    getObject(p: QName, opts: BeingCreated): _IRelation;
    getObject(p: QName, opts?: QueryObjOpts): _IRelation | null;

    getOwnObject(name: string): _IRelation | null;

    parseType(t: string): _IType;


    getType(t: TypeQuery): _IType;
    getType(_t: TypeQuery, opts?: QueryObjOpts): _IType | null;

    getOwnType(name: DataTypeDef): _IType | null

    getObjectByRegClassId(reg: number): _IRelation;
    getObjectByRegClassId(reg: number, opts?: QueryObjOpts): _IRelation | null;

    getOwnObjectByRegClassId(reg: number): _IRelation | null;

    getObjectByRegOrName(reg: RegClass): _IRelation;
    getObjectByRegOrName(reg: RegClass, opts?: QueryObjOpts): _IRelation | null;

    setReadonly(): void;

    _registerTypeSizeable(name: string, type: (sz?: number) => _IType): this;
    _registerType(type: _IType): this;
    _unregisterType(type: _IType): this;

    _reg_register(rel: _IRelation): Reg;
    _reg_unregister(rel: _IRelation): void;
    _reg_rename(rel: _IRelation, oldName: string, newName: string): void;

}


export interface _IStatement {
    readonly schema: _ISchema;
    onExecuted(callback: OnStatementExecuted): void;
}

export interface _IStatementExecutor {
    execute(t: _Transaction): StatementResult;
}

export interface StatementResult {
    result: QueryResult;
    state: _Transaction;
}

export type OnStatementExecuted = (t: _Transaction) => void;

export interface QueryObjOpts extends Partial<BeingCreated> {
    /** Returns null instead of throwing error if not found */
    nullIfNotFound?: boolean;
    /** Will only search in the current schema, or in the targeted schema (not in search path) */
    skipSearch?: boolean;
}

export interface BeingCreated {
    beingCreated: _IRelation;
}

export interface _FunctionDefinition {
    name: string;
    args: _ArgDefDetails[];
    argsVariadic?: _IType | nil;
    returns?: _IType | nil;
    impure?: boolean;
    allowNullArguments?: boolean;
    implementation: (...args: any[]) => any;
}

export interface _OperatorDefinition extends _FunctionDefinition {
    commutative: boolean;
    left: _IType;
    right: _IType;
    returns: _IType;
}

export type _ArgDefDetails = ArgDefDetails & {
    type: _IType;
    default?: IValue;
};

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
    /** Set data persisted in this transaction */
    set<T>(identity: symbol, data: T): T;
    /** Get data persisted in this transaction */
    get<T>(identity: symbol): T;
    getMap<T extends ImMap<any, any>>(identity: symbol): T;
    getSet<T>(identity: symbol): ImSet<T>;
    /** Get transient data, which will only exist within the scope of the current statement */
    setTransient<T>(identity: symbol, data: T): T;
    /** Set transient data, which will only exist within the scope of the current statement */
    getTransient<T>(identity: symbol): T;
    clearTransientData(): void;
}

export interface Stats {
    /** Returns this selection size, or null if it cannot be computed without iteration */
    count: number;
}

export interface _IAggregation {
    checkIfIsKey(got: IValue): IValue;
    getAggregation(name: string, call: ExprCall): IValue;
}

export interface _ISelection<T = any> extends _IAlias {
    readonly debugId?: string;

    readonly ownerSchema: _ISchema;
    readonly db: _IDb;
    /** Tells if this statement is an execution without any meaningful result ("update" with no "returning", etc...) */
    readonly isExecutionWithNoResult: boolean;
    /** Column list (those visible when select *) */
    readonly columns: ReadonlyArray<IValue>;
    /** True when this is an aggregation being built */
    isAggregation(): this is _IAggregation;
    /** Statistical measure of how many items will be returned by this selection */
    entropy(t: _Transaction): number;
    enumerate(t: _Transaction): Iterable<T>;

    /** Returns true if the given value is present in this */
    hasItem(value: T, t: _Transaction): boolean;

    stats(t: _Transaction): Stats | null;

    /** Gets the index associated with this value (or returns null) */
    getIndex(...forValue: IValue[]): _IIndex<T> | nil;
    /** All columns. A bit like .columns`, but including records selections */
    listSelectableIdentities(): Iterable<IValue>;
    filter(where: Expr | nil): _ISelection;
    limit(limit: LimitStatement): _ISelection;
    orderBy(orderBy: OrderByStatement[] | nil): _ISelection;
    groupBy(grouping: Expr[] | nil): _ISelection;
    distinct(select?: Expr[]): _ISelection;
    union(right: _ISelection): _ISelection;
    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    setAlias(alias?: string): _ISelection;
    isOriginOf(a: IValue): boolean;
    explain(e: _Explainer): _SelectExplanation;

    /** Select a specific subset */
    select(select: (string | SelectedColumn)[]): _ISelection;

    /** Limit selection to a specific alias (in joins) */
    selectAlias(alias: string): _IAlias | nil;
}

export interface _IAlias {
    listColumns(): Iterable<IValue>;
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
    take?: _ExprExplanation;
    skip?: _ExprExplanation;
    on: _SelectExplanation;
} | {
    /** A selection transformation */
    id: string | number;
    _: 'distinct';
    of: _SelectExplanation;
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
    getSchema(db?: string | null, nullIfNotFound?: false): _ISchema;
    getSchema(db: string, nullIfNotFound: true): _ISchema | null;
    raiseTable(table: string, event: TableEvent): void;
    raiseGlobal(event: GlobalEvent, ...args: any[]): void;
    listSchemas(): _ISchema[];
    onSchemaChange(): void;
    getTable(name: string, nullIfNotExists?: boolean): _ITable;
    getExtension(name: string): (schema: ISchema) => void;
    /** Get functions matching this overload */
    resolveFunction(name: string | QName, types: IValue[]): _FunctionDefinition | nil;
    /** Get operators matching this overload */
    resolveOperator(name: BinaryOperator, left: IValue, right: IValue): _OperatorDefinition | nil;
    getLanguage(name: string): LanguageCompiler;
}
export type OnConflictHandler = { ignore: 'all' | _IIndex } | {
    onIndex: _IIndex;
    update: (item: any, excluded: any, t: _Transaction) => void;
}

export type DropHandler = (t: _Transaction, cascade: boolean) => void;
export type TruncateHandler = (t: _Transaction, opts: TruncateOpts) => void;
export type IndexHandler = (act: 'create' | 'drop', idx: _INamedIndex) => void;

export interface _RelationBase {
    readonly name: string;
    readonly reg: Reg;
    readonly ownerSchema?: _ISchema;
}

export interface Reg {
    readonly typeId: number;
    readonly classId: number;
}

export interface ChangeOpts {
    onConflict?: OnConflictHandler | nil;
    overriding?: 'user' | 'system' | nil;
}

export interface _ITable<T = any> extends IMemoryTable, _RelationBase {
    readonly type: 'table';
    readonly hidden: boolean;
    readonly db: _IDb;
    readonly selection: _ISelection<T>;
    readonly ownerSchema: _ISchema;
    doInsert(t: _Transaction, toInsert: T, opts?: ChangeOpts): T | null;
    setHidden(): this;
    setReadonly(): this;
    delete(t: _Transaction, toDelete: T): void;
    update(t: _Transaction, toUpdate: T): T;
    createIndex(t: _Transaction, expressions: CreateIndexDef): _IConstraint | nil;
    createIndex(t: _Transaction, expressions: Name[], type: 'primary' | 'unique', indexName?: string | nil): _IConstraint;
    setReadonly(): this;
    /** Create a column */
    addColumn(column: SchemaField | CreateColumnDef, t: _Transaction): _Column;
    /** Get a column to modify it */
    getColumnRef(column: string): _Column;
    getColumnRef(column: string, nullIfNotFound?: boolean): _Column | nil;
    rename(to: string): this;
    getConstraint(constraint: string): _IConstraint | nil;
    addConstraint(constraint: TableConstraint, t: _Transaction): _IConstraint | nil;
    getIndex(...forValues: IValue[]): _IIndex | nil;
    dropIndex(t: _Transaction, name: string): void;
    drop(t: _Transaction, cascade: boolean): void;
    /** Will be executed when one of the given columns is affected (update/delete) */
    onBeforeChange(columns: (string | _Column)[], check: ChangeHandler<T>): ISubscription;
    /** Will be executed once all 'onBeforeChange' handlers have ran (coherency checks) */
    onCheckChange(columns: 'all' | (string | _Column)[], check: ChangeHandler<T>): ISubscription;
    onDrop(sub: DropHandler): ISubscription;
    onIndex(sub: IndexHandler): ISubscription;
    onTruncate(sub: TruncateHandler): ISubscription;
    truncate(t: _Transaction, truncateOpts?: TruncateOpts): void;
}

export interface TruncateOpts {
    restartIdentity?: boolean;
    cascade?: boolean;
}

export interface _IView extends _RelationBase {
    readonly type: 'view';
    readonly db: _IDb;
    readonly selection: _ISelection;
    drop(t: _Transaction): void;
}


export interface _IConstraint {
    readonly name: string | nil;
    uninstall(t: _Transaction): void;
}

export type ChangeHandler<T = any> = (old: T | null, neu: T | null, t: _Transaction, opts: ChangeOpts) => void;

export interface _Column {
    readonly notNull: boolean;
    readonly default: IValue | nil;
    readonly expression: IValue;
    readonly usedInIndexes: ReadonlySet<_IIndex>;
    readonly table: _ITable;
    readonly name: string;
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
    predicate?: IValue;
}
export interface CreateIndexColDef {
    value: IValue;
    nullsLast?: boolean;
    desc?: boolean
}


export interface _IType<TRaw = any> extends IType, _RelationBase {
    readonly type: 'type';
    /** Data type */
    readonly primary: DataType;
    /** Reg type name */
    readonly name: string; // | null;
    readonly reg: Reg;

    toString(): string;
    equals(a: TRaw, b: TRaw): boolean | null;
    gt(a: TRaw, b: TRaw): boolean | null;
    ge(a: TRaw, b: TRaw): boolean | null;
    lt(a: TRaw, b: TRaw): boolean | null;
    le(a: TRaw, b: TRaw): boolean | null;
    canConvertImplicit(to: _IType<TRaw>): boolean | nil;
    canCast(to: _IType<TRaw>): boolean | nil;
    cast<T = any>(value: IValue<TRaw>, to: _IType<T>): IValue<T>;
    convertImplicit<T = any>(value: IValue<TRaw>, to: _IType<T>): IValue<T>;
    prefer(type: _IType<any>): _IType | nil;

    /** Build an array type for this type */
    asArray(): _IType<TRaw[]>;
    asList(): _IType<TRaw[]>;

    /** Get an unicity hash */
    hash(value: TRaw): string | number | null;

    drop(t: _Transaction): void;
}

export interface Parameter {
    readonly index: number;
    readonly value: IValue;
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

    /** Get value if is a constant */
    get(): any;
    /** Get value if is NOT a constant */
    get(raw: TRaw, t?: _Transaction | nil): any;

    setId(newId: string): IValue;
    canCast(to: _IType): boolean;
    cast<T = any>(to: _IType<T>): IValue<T>;
    convertImplicit<T = any>(to: _IType<T>): IValue<T>;

    /**
     * Creates a copy of this column that can
     **/
    setWrapper<TNew>(newOrigin: _ISelection, unwrap: (val: TRaw) => TNew, newType: _IType<TNew>): IValue<TNew>;
    setWrapper(newOrigin: _ISelection, unwrap: (val: TRaw) => TRaw): IValue<TRaw>;
    map(unwrap: (val: TRaw) => TRaw): IValue<TRaw>;
    map<TNew>(unwrap: (val: TRaw) => TNew, newType: _IType<TNew>): IValue<TNew>;
    setOrigin(origin: _ISelection): IValue<TRaw>;
    clone(): IValue<any>;

    explain(e: _Explainer): _ExprExplanation;
}

export type IndexKey = unknown[];
export interface IndexExpression {
    readonly hash: string;
    readonly type: _IType;
}

export interface _INamedIndex<T = any> extends _IIndex<T>, _RelationBase {
    readonly type: 'index';
    readonly onTable: _ITable<T>;
    drop(t: _Transaction): void;
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
    readonly?: boolean;
    hidden?: boolean;
    name?: string;
    dataId?: symbol;

    serials: ImMap<string, number>;
    it: number;
    indexByHash: ImMap<string, _IIndex<T>>;
    indexByName: ImMap<string, _IIndex<T>>;
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
    columnsByName: ImMap(),
});

export const NewColumn = Record<TableColumnRecordDef<any>>({
    default: null as any,
    notNull: false,
    usedInIndexes: ImSet(),
    type: null as any,
    name: null as any,
});

export type _IRelation = _ITable | _ISequence | _INamedIndex | _IType | _IView;

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

export function asType(o: _IRelation): _IType;
export function asType(o: _IRelation | null): _IType | null;
export function asType(o: _IRelation | null) {
    if (!o) {
        return null;
    }
    if (o.type === 'type') {
        return o;
    }
    throw new QueryError(`"${o.name}" is not a type`);
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


export type _ISelectable = _ITable | _IView;
export function asSelectable(o: _IRelation): _ISelectable;
export function asSelectable(o: _IRelation | null): _ISelectable | null;
export function asSelectable(o: _IRelation | null, nullIfNotType?: boolean): _ISelectable | null;
export function asSelectable(o: _IRelation | null, nullIfNotType?: boolean) {
    if (!o) {
        return null;
    }
    if (o.type === 'table' || o.type === 'view') {
        return o;
    }
    if (nullIfNotType) {
        return null;
    }
    throw new QueryError(`"${o.name}" is not selectable`);
}


export function asView(o: _IRelation): _IView;
export function asView(o: _IRelation | null): _IView | null;
export function asView(o: _IRelation | null, nullIfNotType?: boolean): _IView | null;
export function asView(o: _IRelation | null, nullIfNotType?: boolean) {
    if (!o) {
        return null;
    }
    if (o.type === 'view') {
        return o;
    }
    if (nullIfNotType) {
        return null;
    }
    throw new QueryError({
        code: '42809',
        error: `"${o.name}" is not a view`,
    });
}

export interface _ISequence extends _RelationBase {

    readonly type: 'sequence';
    alter(t: _Transaction, opts: CreateSequenceOptions | AlterSequenceChange): this;
    nextValue(t: _Transaction): number;
    restart(t: _Transaction): void;
    setValue(t: _Transaction, value: number): void;
    currentValue(t: _Transaction): number;
    drop(t: _Transaction): void;
}


export interface AggregationComputer<TRet = any> {
    readonly type: _IType;
    /**  Compute from index  (ex: count(*) with a group-by) */
    computeFromIndex?(key: IndexKey, index: _IIndex, t: _Transaction): TRet | undefined;
    /**  Compute out of nowhere when there is no group
     * (ex: when there is no grouping, count(*) on a table or count(xxx) when there is an index on xxx) */
    computeNoGroup?(t: _Transaction): TRet | undefined;

    /** When iterating, each new group will have its computer */
    createGroup(t: _Transaction): AggregationGroupComputer<TRet>;
}

export interface AggregationGroupComputer<TRet = any> {
    /** When iterating, this will be called for each item in this group  */
    feedItem(item: any): void;
    /** Finish computation (sets aggregation on result) */
    finish(): TRet | nil;
}
