export interface CreateIndexStatement {
    type: 'create index';
    table: string;
    expressions: IndexExpression[];
    unique?: true;
    ifNotExists?: true;
    indexName?: string;
}

export interface IndexExpression {
    expression: Value;
    order?: 'asc' | 'desc';
    nulls?: 'first' | 'last';
}

export interface CreateTableStatement {
    type: 'create table';
    name: string;
    ifNotExists?: true;
    columns: CreateColumnDef[];
}

export interface CreateColumnDef {
    name: string;
    dataType: string;
    // collate?: string; (todo)
    constraint?: ColumnConstraint;
}

type ColumnConstraint = {
    type: 'unique';
    notNull?: boolean;
} | {
    type: 'primary key';
}

export interface SelectStatement {
    type: 'select',
    columns?: Value[];
    from?: From;
    where?: Value;
}

export type From = {
    subject: string;
    alias?: string;
} | {
    subject: SelectStatement;
    alias: string;
}

export type Value = ValueRef
    | ValueStar
    | ValueInteger
    | ValueMember
    | ValueArrayIndex
    | ValueNumeric
    | ValueString
    | ValueBinary
    | ValueUnary
    | ValueCast
    | ValueBool
    | ValueCall
    | ValueTernary;


export type LogicOperator = 'OR' | 'AND';
export type EqualityOperator = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';
export type ComparisonOperator = '>' | '>=' | '<' | '<=' | '@>' | '<@' | '?' | '?|' | '?&';
export type AdditiveOperator = '||' | '-' | '#-' | '&&' | '+';
export type MultiplicativeOperator = '*' | '%' | '/';
export type BinaryOperator = LogicOperator
    | EqualityOperator
    | ComparisonOperator
    | AdditiveOperator
    | MultiplicativeOperator
    | '^';

export interface ValueBinary {
    type: 'binary';
    left: Value;
    right: Value;
    op: BinaryOperator;
}


export interface ValueTernary {
    type: 'ternary';
    value: Value;
    lo: Value;
    hi: Value;
    op: 'BETWEEN' | 'NOT BETWEEN';
}

export interface ValueCast {
    type: 'cast';
    to: string;
    operand: Value;
}

export interface ValueUnary {
    type: 'unary';
    operand: Value;
    op: '+' | '-' | 'NOT' | 'IS NULL' | 'IS NOT NULL' | 'IS TRUE' | 'IS FALSE' | 'IS NOT TRUE' | 'IS NOT FALSE';
}

export interface ValueRef {
    type: 'ref';
    name: string;
}

export interface ValueMember {
    type: 'member';
    operand: Value;
    /** If not provided, then is a classic member access with '.' */
    op?: '->' | '->>'; // <== todo
    member: '*' | string | number;
}

export interface ValueCall {
    type: 'call';
    function: string;
    args: Value[];
}

export interface ValueArrayIndex {
    type: 'arrayIndex',
    array: Value;
    index: Value;
}

export interface ValueStar {
    type: 'star';
}

export interface ValueInteger {
    type: 'integer';
    value: number;
}

export interface ValueNumeric {
    type: 'numeric';
    value: number;
}

export interface ValueString {
    type: 'string';
    value: string;
}

export interface ValueBool {
    type: 'boolean';
    value: boolean;
}
